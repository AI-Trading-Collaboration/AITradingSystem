from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

import ai_trading_system.dynamic_strategy_blocking_gap_remediation_implementation_plan as m2408
import ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision as m2391
import ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification as m2390
import ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision as m2389
import ai_trading_system.dynamic_strategy_component_ablation_owner_review_decision as m2394
import ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan as m2392
import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as m2393
import ai_trading_system.dynamic_strategy_component_recombination_candidate_plan as m2395
import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as m2396
import ai_trading_system.dynamic_strategy_data_pit_signal_quality_gap_review as m2402
import ai_trading_system.dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411
import ai_trading_system.dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan as m2406
import ai_trading_system.dynamic_strategy_pit_coverage_matrix_implementation_plan as m2404
import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as m2405
import ai_trading_system.dynamic_strategy_pit_coverage_signal_construction_review as m2403
import ai_trading_system.dynamic_strategy_recombination_candidate_gate_evidence_plan as m2398
import ai_trading_system.dynamic_strategy_recombination_candidate_owner_review_decision as m2397
import ai_trading_system.dynamic_strategy_recombination_line_plateau_decision as m2401
import ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review as m2388
import ai_trading_system.dynamic_strategy_signal_as_of_validity_contract_schema as m2409
import ai_trading_system.dynamic_strategy_targeted_gate_evidence_owner_review_decision as m2400
import ai_trading_system.dynamic_strategy_valid_until_window_stale_signal_remediation_plan as m2407
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system import (
    dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest as m2399,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT,
    DEFAULT_SOURCE_OWNER_REASSESSMENT_PATH,
    run_dynamic_strategy_candidate_optimization_divergence_review,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH as DEFAULT_OPT_CANDIDATE_CMP,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_SOURCE_DECISION_UPDATE_PATH as DEFAULT_OPT_SOURCE_DECISION_UPDATE,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH as DEFAULT_OPT_SOURCE_OWNER_GATE,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH as DEFAULT_OPT_SOURCE_SENSITIVITY_MATRIX,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH as DEFAULT_OPT_SOURCE_SENSITIVITY_RESULT,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_OUTPUT_ROOT,
    run_dynamic_strategy_candidate_pool_expansion_plan,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2385_SOURCE_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2385_SOURCE_SENS_RESULT,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2385_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2385_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH as DEFAULT_2385_SOURCE_GUARDED_DECISION,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH as DEFAULT_2385_SOURCE_GUARDED_RANKING,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH as DEFAULT_2385_SOURCE_GUARDED_RETEST,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2384_NEXT_DIRECTION_PATH as DEFAULT_2385_SOURCE_NEXT_DIRECTION,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH as DEFAULT_2385_SOURCE_OWNER_REVIEW,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH as DEFAULT_2385_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    run_dynamic_strategy_cost_turnover_cooldown_sensitivity,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT,
    DEFAULT_SOURCE_CADENCE_AUDIT_PATH,
    run_dynamic_strategy_event_driven_retest,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT,
    run_dynamic_strategy_execution_cadence_bias_audit,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH,
    run_dynamic_strategy_expanded_candidate_pool_retest,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2386_SOURCE_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2386_SOURCE_SENS_RESULT,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2386_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2386_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH as DEFAULT_2386_SOURCE_GUARDED_DECISION,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH as DEFAULT_2386_SOURCE_GUARDED_RANKING,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH as DEFAULT_2386_SOURCE_GUARDED_RETEST,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH as DEFAULT_2386_SOURCE_OWNER_REVIEW,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH as DEFAULT_2386_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    run_dynamic_strategy_guarded_variant_owner_review_decision,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2384_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2384_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH as DEFAULT_2384_SOURCE_OBSERVATION_REJECTION,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH as DEFAULT_2384_SOURCE_OWNER_REVIEW,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2381_NEXT_DIRECTION_PATH as DEFAULT_2384_SOURCE_NEXT_DIRECTION,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2381_PLATEAU_DECISION_PATH as DEFAULT_2384_SOURCE_PLATEAU_DECISION,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2382_GUARDED_VARIANT_PLAN_PATH as DEFAULT_2384_SOURCE_GUARDED_PLAN,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2382_RETEST_PLAN_PATH as DEFAULT_2384_SOURCE_RETEST_PLAN,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH as DEFAULT_2384_SOURCE_GUARDED_DECISION,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH as DEFAULT_2384_SOURCE_GUARDED_RANKING,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH as DEFAULT_2384_SOURCE_GUARDED_RETEST,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT,
    run_dynamic_strategy_observation_gate_threshold_calibration_review,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2387_SOURCE_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2387_SOURCE_SENS_RESULT,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH as DEFAULT_2387_SOURCE_OWNER_REVIEW,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH as DEFAULT_2387_SOURCE_POOL_PLAN,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH as DEFAULT_2387_SOURCE_DECISION_UPDATE,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH as DEFAULT_2387_SOURCE_RANKING,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH as DEFAULT_2387_SOURCE_RETEST,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH as DEFAULT_2387_SOURCE_SIGNAL_FAMILY,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH as DEFAULT_2387_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT,
    run_dynamic_strategy_optimization_plateau_next_candidate_decision,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH as DEFAULT_2381_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2381_SOURCE_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2381_SOURCE_SENS_RESULT,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH as DEFAULT_2381_SOURCE_TARGETED_RETEST,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2381_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2381_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH as DEFAULT_2381_SOURCE_OBSERVATION_REJECTION,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH as DEFAULT_2381_SOURCE_OWNER_REVIEW,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
    run_dynamic_strategy_optimized_candidate_targeted_retest,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_TARGETED_RETEST_SOURCE_2366_DECISION_UPDATE,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_SOURCE_2366_MATRIX_PATH as DEFAULT_TARGETED_RETEST_SOURCE_2366_MATRIX,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_SOURCE_2366_RESULT_PATH as DEFAULT_TARGETED_RETEST_SOURCE_2366_RESULT,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH as DEFAULT_TARGETED_RETEST_SOURCE_2375_DECISION_UPDATE,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_SOURCE_2375_OPTIMIZATION_MATRIX_PATH as DEFAULT_TARGETED_RETEST_SOURCE_2375_MATRIX,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_SOURCE_2375_RESULT_PATH as DEFAULT_TARGETED_RETEST_SOURCE_2375_RESULT,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    run_dynamic_strategy_optimized_variant_owner_review_decision,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH as DEFAULT_2380_SOURCE_TARGETED_DECISION,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH as DEFAULT_2380_SOURCE_TARGETED_RETEST,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2378_OPTIMIZATION_PLAN_PATH as DEFAULT_2380_SOURCE_OPTIMIZATION_PLAN,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2378_VARIANT_EVALUATION_PLAN_PATH as DEFAULT_2380_SOURCE_VARIANT_EVALUATION_PLAN,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2379_DECISION_UPDATE_PATH as DEFAULT_2380_SOURCE_VARIANT_DECISION,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2380_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2380_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT,
    run_dynamic_strategy_ranking_top_guarded_turnover_retest_plan,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH as DEFAULT_2382_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2382_SOURCE_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2382_SOURCE_SENS_RESULT,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2382_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2382_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH as DEFAULT_2382_SOURCE_OBSERVATION_REJECTION,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH as DEFAULT_2382_SOURCE_OWNER_REVIEW,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2381_NEXT_DIRECTION_PATH as DEFAULT_2382_SOURCE_NEXT_DIRECTION,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    DEFAULT_SOURCE_2381_PLATEAU_DECISION_PATH as DEFAULT_2382_SOURCE_PLATEAU_DECISION,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT,
    run_dynamic_strategy_ranking_top_guarded_variant_retest,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH as DEFAULT_2383_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2383_SOURCE_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2383_SOURCE_SENS_RESULT,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH as DEFAULT_2383_SOURCE_VARIANT_RANKING,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH as DEFAULT_2383_SOURCE_VARIANT_RETEST,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2382_GUARDED_VARIANT_PLAN_PATH as DEFAULT_2383_SOURCE_GUARDED_PLAN,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2382_RETEST_PLAN_PATH as DEFAULT_2383_SOURCE_RETEST_PLAN,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_SOURCE_2382_VARIANT_EVALUATION_PLAN_PATH as DEFAULT_2383_SOURCE_EVALUATION_PLAN,
)
from ai_trading_system.dynamic_strategy_research_only_observation_log_schema_plan import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_OUTPUT_ROOT,
    DEFAULT_SOURCE_OWNER_REVIEW_DECISION_PATH,
    run_dynamic_strategy_research_only_observation_log_schema_plan,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    DEFAULT_SOURCE_REPLAY_VALIDATION_PATH,
    run_dynamic_strategy_research_only_shadow_observation_owner_review_decision,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_reassessment import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_OUTPUT_ROOT,
    DEFAULT_SOURCE_2373_REPORT_DRY_RUN_PATH,
    run_dynamic_strategy_research_only_observation_owner_reassessment,
)
from ai_trading_system.dynamic_strategy_research_only_observation_report_dry_run import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_OUTPUT_ROOT,
    DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RECORD_PATH,
    DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RESULT_PATH,
    DEFAULT_SOURCE_2370_REPLAY_VALIDATION_PATH,
    DEFAULT_SOURCE_2371_OWNER_REVIEW_DECISION_PATH,
    DEFAULT_SOURCE_2372_LOG_SCHEMA_PLAN_PATH,
    run_dynamic_strategy_research_only_observation_report_dry_run,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT,
    DEFAULT_SOURCE_OBSERVATION_FIELD_SCHEMA_PATH,
    DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH,
    DEFAULT_SOURCE_REVIEW_THRESHOLDS_PATH,
    run_dynamic_strategy_research_only_shadow_observation_dry_run,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH,
    DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    DEFAULT_SOURCE_SHADOW_RESEARCH_GATE_DECISION_PATH,
    run_dynamic_strategy_research_only_shadow_observation_protocol,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT,
    DEFAULT_REPLAY_COUNT,
    DEFAULT_SOURCE_DRY_RUN_NO_SIDE_EFFECT_EVIDENCE_PATH,
    DEFAULT_SOURCE_DRY_RUN_RECORD_PATH,
    DEFAULT_SOURCE_DRY_RUN_RESULT_PATH,
    run_dynamic_strategy_research_only_shadow_observation_replay_validation,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH as DEFAULT_REPLAY_SOURCE_OBSERVATION_PROTOCOL_PATH,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH as DEFAULT_REPLAY_SOURCE_OWNER_REVIEW_GATE_PATH,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
    run_dynamic_strategy_slice_robustness_optimized_variant_retest,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH as DEFAULT_2379_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2379_SOURCE_SENSITIVITY_RESULT,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH as DEFAULT_2379_OPT_DECISION,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH as DEFAULT_2379_SOURCE_OPTIMIZATION_REVIEW,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH as DEFAULT_2379_SOURCE_TARGETED_DECISION,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH as DEFAULT_2379_SOURCE_TARGETED_RETEST,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2378_OPTIMIZATION_PLAN_PATH as DEFAULT_2379_SOURCE_OPTIMIZATION_PLAN,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_SOURCE_2378_VARIANT_EVALUATION_PLAN_PATH as DEFAULT_2379_SOURCE_VARIANT_EVALUATION_PLAN,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT,
    run_dynamic_strategy_slice_robustness_return_gap_optimization_plan,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH as DEFAULT_2378_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2365_EVENT_RETEST_PATH as DEFAULT_2378_SOURCE_EVENT_RETEST,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2366_SENSITIVITY_DECISION_UPDATE_PATH as DEFAULT_2378_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2378_SOURCE_SENSITIVITY_RESULT,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2375_OPTIMIZATION_DECISION_UPDATE_PATH as DEFAULT_2378_OPT_DECISION,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH as DEFAULT_2378_SOURCE_OPTIMIZATION_REVIEW,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2376_TARGETED_DECISION_UPDATE_PATH as DEFAULT_2378_SOURCE_TARGETED_DECISION,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH as DEFAULT_2378_SOURCE_TARGETED_RETEST,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_SOURCE_2377_OWNER_REVIEW_DECISION_PATH as DEFAULT_2378_SOURCE_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    run_dynamic_strategy_targeted_retest_owner_review_decision,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH as DEFAULT_2377_SOURCE_CANDIDATE_RANKING,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2365_EVENT_RETEST_PATH as DEFAULT_2377_SOURCE_EVENT_RETEST,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2366_SENSITIVITY_DECISION_UPDATE_PATH as DEFAULT_2377_SENS_DECISION,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH as DEFAULT_2377_SOURCE_SENSITIVITY_RESULT,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2375_OPTIMIZATION_DECISION_UPDATE_PATH as DEFAULT_2377_OPT_DECISION,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH as DEFAULT_2377_SOURCE_OPTIMIZATION_REVIEW,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2376_TARGETED_DECISION_UPDATE_PATH as DEFAULT_2377_SOURCE_TARGETED_DECISION,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH as DEFAULT_2377_SOURCE_TARGETED_RETEST,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_DOCS_ROOT,
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT,
    DEFAULT_SOURCE_DECISION_UPDATE_PATH,
    DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH,
    DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    run_dynamic_strategy_top_candidate_owner_review_gate,
)
from ai_trading_system.execution_semantics import (
    DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_REVIEW_PATH,
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_ARTIFACT_GOVERNANCE_OUTPUT_ROOT,
    DEFAULT_CASH_YIELD_MODEL_PATH,
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    DEFAULT_COST_CASH_YIELD_OUTPUT_ROOT,
    DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_DOC_PATH,
    DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_YAML_PATH,
    DEFAULT_DYNAMIC_POLICY_SENSITIVITY_DOC_PATH,
    DEFAULT_DYNAMIC_POLICY_SENSITIVITY_YAML_PATH,
    DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_MATRIX_YAML_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_REVIEW_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_MATRIX_YAML_PATH,
    DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_REVIEW_PATH,
    DEFAULT_DYNAMIC_WALK_FORWARD_POLICY_PATH,
    DEFAULT_EDGE_ATTRIBUTION_OUTPUT_ROOT,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_CONFIG_PATH,
    DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_REVIEW_PATH,
    DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_SNAPSHOT_PATH,
    DEFAULT_EVENT_OVERRIDE_EXECUTION_SEMANTICS_REVIEW_PATH,
    DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    DEFAULT_EVENT_OVERRIDE_SURVIVAL_MATRIX_YAML_PATH,
    DEFAULT_EVENT_TAXONOMY_OUTPUT_ROOT,
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    DEFAULT_EXECUTION_REBACKTEST_STRATEGY_IDS,
    DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    DEFAULT_LAYER1_SELECTOR_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PIT_AUDIT_OUTPUT_ROOT,
    DEFAULT_PIT_DATA_AVAILABILITY_AUDIT_REVIEW_PATH,
    DEFAULT_PIT_DATA_AVAILABILITY_INVENTORY_PATH,
    DEFAULT_POLICY_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_PRICES_PATH,
    DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_REGIME_BASELINE_EXPANSION_MATRIX_YAML_PATH,
    DEFAULT_REGIME_BASELINE_EXPANSION_POLICY_PATH,
    DEFAULT_REGIME_BASELINE_EXPANSION_REVIEW_PATH,
    DEFAULT_REGIME_BASELINE_OUTPUT_ROOT,
    DEFAULT_RESEARCH_ARTIFACT_GOVERNANCE_REVIEW_PATH,
    DEFAULT_RESEARCH_ARTIFACT_GOVERNANCE_SNAPSHOT_PATH,
    DEFAULT_RISK_TIMING_QUALITY_MATRIX_YAML_PATH,
    DEFAULT_RISK_TIMING_QUALITY_POLICY_PATH,
    DEFAULT_RISK_TIMING_QUALITY_REVIEW_PATH,
    DEFAULT_SIGNAL_VALIDITY_STALENESS_INPUT_SUMMARY_PATH,
    DEFAULT_SIGNAL_VALIDITY_STALENESS_REPAIR_REVIEW_PATH,
    DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    DEFAULT_STALENESS_REPAIR_MATRIX_YAML_PATH,
    DEFAULT_STRESS_RISK_METRICS_MATRIX_YAML_PATH,
    DEFAULT_STRESS_RISK_METRICS_POLICY_PATH,
    DEFAULT_STRESS_RISK_METRICS_REVIEW_PATH,
    DEFAULT_STRESS_RISK_OUTPUT_ROOT,
    DEFAULT_TIMING_QUALITY_OUTPUT_ROOT,
    DEFAULT_TRANSACTION_COST_CASH_YIELD_MATRIX_YAML_PATH,
    DEFAULT_TRANSACTION_COST_CASH_YIELD_REVIEW_PATH,
    DEFAULT_TRANSACTION_COST_MODEL_PATH,
    DEFAULT_WALK_FORWARD_OUTPUT_ROOT,
    EVENT_OVERRIDE_MODE_T_PLUS_1,
    run_actual_path_edge_attribution_review,
    run_dynamic_actual_path_owner_review_decision,
    run_dynamic_actual_path_policy_sensitivity_review,
    run_dynamic_backtest_engine_contract_update,
    run_dynamic_strategy_execution_semantics_contract,
    run_dynamic_strategy_latency_execution_lag_review,
    run_dynamic_strategy_objective_gate_review,
    run_dynamic_strategy_validity_period_audit,
    run_dynamic_strategy_walk_forward_validation,
    run_equal_risk_balanced_core_execution_policy_selection,
    run_event_override_ex_ante_taxonomy_review,
    run_execution_aware_forward_aging_observation_contract,
    run_execution_policy_cost_turnover_normalization,
    run_execution_policy_impact_on_prior_conclusions,
    run_execution_semantics_data_lineage_audit,
    run_execution_semantics_external_validation_update,
    run_execution_semantics_master_review,
    run_execution_semantics_rebacktest,
    run_execution_semantics_rebacktest_gate,
    run_execution_semantics_reporting_update,
    run_implicit_monthly_rebalance_assumption_audit,
    run_pit_data_availability_audit,
    run_reader_brief_execution_semantics_safe_preview,
    run_rebalance_assumption_owner_review_pack,
    run_rebalance_frequency_sensitivity_suite,
    run_rebalance_sensitive_candidate_recovery_review,
    run_regime_segmentation_baseline_expansion_review,
    run_research_artifact_governance_review,
    run_risk_timing_quality_review,
    run_roadmap_update_after_execution_semantics_review,
    run_signal_staleness_cost_review,
    run_strategy_execution_policy_registry_review,
    run_stress_risk_metrics_review,
    run_target_vs_actual_position_path_builder,
    run_threshold_hybrid_rebalance_review,
    run_transaction_cost_cash_yield_audit,
)

console = Console()


def register_execution_semantics_strategy_commands(strategies_app: typer.Typer) -> None:
    strategies_app.command("execution-semantics-rebacktest")(
        _execution_semantics_rebacktest_command
    )
    strategies_app.command("dynamic-actual-path-owner-review-decision")(
        _dynamic_actual_path_owner_review_decision_command
    )
    strategies_app.command("dynamic-actual-path-policy-sensitivity-review")(
        _dynamic_actual_path_policy_sensitivity_review_command
    )
    strategies_app.command("actual-path-edge-attribution")(
        _actual_path_edge_attribution_command
    )
    strategies_app.command("dynamic-strategy-objective-gate-review")(
        _dynamic_strategy_objective_gate_review_command
    )
    strategies_app.command("pit-data-availability-audit")(
        _pit_data_availability_audit_command
    )
    strategies_app.command("dynamic-strategy-walk-forward-validation")(
        _dynamic_strategy_walk_forward_validation_command
    )
    strategies_app.command("event-override-ex-ante-taxonomy-review")(
        _event_override_ex_ante_taxonomy_review_command
    )
    strategies_app.command("risk-timing-quality-review")(
        _risk_timing_quality_review_command
    )
    strategies_app.command("transaction-cost-cash-yield-audit")(
        _transaction_cost_cash_yield_audit_command
    )
    strategies_app.command("stress-risk-metrics-review")(
        _stress_risk_metrics_review_command
    )
    strategies_app.command("regime-segmentation-baseline-expansion-review")(
        _regime_segmentation_baseline_expansion_review_command
    )
    strategies_app.command("research-artifact-governance-review")(
        _research_artifact_governance_review_command
    )
    strategies_app.command("dynamic-strategy-execution-cadence-bias-audit")(
        _dynamic_strategy_execution_cadence_bias_audit_command
    )
    strategies_app.command("dynamic-strategy-event-driven-retest")(
        _dynamic_strategy_event_driven_retest_command
    )
    strategies_app.command("dynamic-strategy-cost-turnover-cooldown-sensitivity")(
        _dynamic_strategy_cost_turnover_cooldown_sensitivity_command
    )
    strategies_app.command("dynamic-strategy-top-candidate-owner-review-gate")(
        _dynamic_strategy_top_candidate_owner_review_gate_command
    )
    strategies_app.command("dynamic-strategy-research-only-shadow-observation-protocol")(
        _dynamic_strategy_research_only_shadow_observation_protocol_command
    )
    strategies_app.command("dynamic-strategy-research-only-shadow-observation-dry-run")(
        _dynamic_strategy_research_only_shadow_observation_dry_run_command
    )
    strategies_app.command(
        "dynamic-strategy-research-only-shadow-observation-replay-validation"
    )(_dynamic_strategy_research_only_shadow_observation_replay_validation_command)
    strategies_app.command(
        "dynamic-strategy-research-only-shadow-observation-owner-review-decision"
    )(_dynamic_strategy_research_only_shadow_observation_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-research-only-observation-log-schema-plan"
    )(_dynamic_strategy_research_only_observation_log_schema_plan_command)
    strategies_app.command(
        "dynamic-strategy-research-only-observation-report-dry-run"
    )(_dynamic_strategy_research_only_observation_report_dry_run_command)
    strategies_app.command(
        "dynamic-strategy-research-only-observation-owner-reassessment"
    )(_dynamic_strategy_research_only_observation_owner_reassessment_command)
    strategies_app.command(
        "dynamic-strategy-candidate-optimization-divergence-review"
    )(_dynamic_strategy_candidate_optimization_divergence_review_command)
    strategies_app.command(
        "dynamic-strategy-optimized-candidate-targeted-retest"
    )(_dynamic_strategy_optimized_candidate_targeted_retest_command)
    strategies_app.command(
        "dynamic-strategy-targeted-retest-owner-review-decision"
    )(_dynamic_strategy_targeted_retest_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-slice-robustness-return-gap-optimization-plan"
    )(_dynamic_strategy_slice_robustness_return_gap_optimization_plan_command)
    strategies_app.command(
        "dynamic-strategy-slice-robustness-optimized-variant-retest"
    )(_dynamic_strategy_slice_robustness_optimized_variant_retest_command)
    strategies_app.command(
        "dynamic-strategy-optimized-variant-owner-review-decision"
    )(_dynamic_strategy_optimized_variant_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-optimization-plateau-next-candidate-decision"
    )(_dynamic_strategy_optimization_plateau_next_candidate_decision_command)
    strategies_app.command(
        "dynamic-strategy-ranking-top-guarded-turnover-retest-plan"
    )(_dynamic_strategy_ranking_top_guarded_turnover_retest_plan_command)
    strategies_app.command(
        "dynamic-strategy-ranking-top-guarded-variant-retest"
    )(_dynamic_strategy_ranking_top_guarded_variant_retest_command)
    strategies_app.command(
        "dynamic-strategy-guarded-variant-owner-review-decision"
    )(_dynamic_strategy_guarded_variant_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-candidate-pool-expansion-plan"
    )(_dynamic_strategy_candidate_pool_expansion_plan_command)
    strategies_app.command(
        "dynamic-strategy-expanded-candidate-pool-retest"
    )(_dynamic_strategy_expanded_candidate_pool_retest_command)
    strategies_app.command(
        "dynamic-strategy-observation-gate-threshold-calibration-review"
    )(_dynamic_strategy_observation_gate_threshold_calibration_review_command)
    strategies_app.command(
        "dynamic-strategy-research-filter-threshold-methodology-review"
    )(_dynamic_strategy_research_filter_threshold_methodology_review_command)
    strategies_app.command(
        "dynamic-strategy-calibrated-gate-owner-review-decision"
    )(_dynamic_strategy_calibrated_gate_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-calibrated-gate-candidate-reclassification"
    )(_dynamic_strategy_calibrated_gate_candidate_reclassification_command)
    strategies_app.command(
        "dynamic-strategy-calibrated-gate-candidate-owner-review-decision"
    )(_dynamic_strategy_calibrated_gate_candidate_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-component-attribution-gate-evidence-plan"
    )(_dynamic_strategy_component_attribution_gate_evidence_plan_command)
    strategies_app.command(
        "dynamic-strategy-component-attribution-targeted-ablation-retest"
    )(_dynamic_strategy_component_attribution_targeted_ablation_retest_command)
    strategies_app.command(
        "dynamic-strategy-component-ablation-owner-review-decision"
    )(_dynamic_strategy_component_ablation_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-component-recombination-candidate-plan"
    )(_dynamic_strategy_component_recombination_candidate_plan_command)
    strategies_app.command(
        "dynamic-strategy-component-recombination-candidate-retest"
    )(_dynamic_strategy_component_recombination_candidate_retest_command)
    strategies_app.command(
        "dynamic-strategy-recombination-candidate-owner-review-decision"
    )(_dynamic_strategy_recombination_candidate_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-recombination-candidate-gate-evidence-plan"
    )(_dynamic_strategy_recombination_candidate_gate_evidence_plan_command)
    strategies_app.command(
        "dynamic-strategy-recombination-candidate-targeted-gate-evidence-retest"
    )(_dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest_command)
    strategies_app.command(
        "dynamic-strategy-targeted-gate-evidence-owner-review-decision"
    )(_dynamic_strategy_targeted_gate_evidence_owner_review_decision_command)
    strategies_app.command(
        "dynamic-strategy-recombination-line-plateau-decision"
    )(_dynamic_strategy_recombination_line_plateau_decision_command)
    strategies_app.command(
        "dynamic-strategy-data-pit-signal-quality-gap-review"
    )(_dynamic_strategy_data_pit_signal_quality_gap_review_command)
    strategies_app.command(
        "dynamic-strategy-pit-coverage-signal-construction-review"
    )(_dynamic_strategy_pit_coverage_signal_construction_review_command)
    strategies_app.command(
        "dynamic-strategy-pit-coverage-matrix-implementation-plan"
    )(_dynamic_strategy_pit_coverage_matrix_implementation_plan_command)
    strategies_app.command(
        "dynamic-strategy-pit-coverage-matrix-generate"
    )(_dynamic_strategy_pit_coverage_matrix_generate_command)
    strategies_app.command(
        "dynamic-strategy-growth-tilt-engine-pit-signal-remediation-plan"
    )(_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan_command)
    strategies_app.command(
        "dynamic-strategy-valid-until-window-stale-signal-remediation-plan"
    )(_dynamic_strategy_valid_until_window_stale_signal_remediation_plan_command)
    strategies_app.command(
        "dynamic-strategy-blocking-gap-remediation-implementation-plan"
    )(_dynamic_strategy_blocking_gap_remediation_implementation_plan_command)
    strategies_app.command(
        "dynamic-strategy-signal-as-of-validity-contract-schema"
    )(_dynamic_strategy_signal_as_of_validity_contract_schema_command)
    strategies_app.command("growth-tilt-engine-source-feature-contract-mapping")(
        _growth_tilt_engine_source_feature_contract_mapping_command
    )
    strategies_app.command("growth-tilt-engine-contract-gap-remediation-plan")(
        _growth_tilt_engine_contract_gap_remediation_plan_command
    )
    for command_name, builder, label in _EXECUTION_SEMANTICS_COMMANDS:
        strategies_app.command(command_name)(_make_execution_semantics_command(builder, label))


def _make_execution_semantics_command(
    builder: Callable[..., dict[str, object]],
    label: str,
) -> Callable[..., None]:
    def command(
        prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
        marketstack_prices_path: Annotated[
            Path, typer.Option("--marketstack-prices-path")
        ] = DEFAULT_MARKETSTACK_PRICES_PATH,
        rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
        simple_config_path: Annotated[
            Path, typer.Option("--simple-config")
        ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
        growth_config_path: Annotated[
            Path, typer.Option("--growth-config")
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        controlled_growth_config_path: Annotated[
            Path, typer.Option("--controlled-growth-config")
        ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
        layer1_config_path: Annotated[
            Path, typer.Option("--layer1-config")
        ] = DEFAULT_LAYER1_SELECTOR_CONFIG_PATH,
        qqq_plus_config_path: Annotated[
            Path, typer.Option("--qqq-plus-config")
        ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
        policy_registry_path: Annotated[
            Path, typer.Option("--policy-registry")
        ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
        output_root: Annotated[
            Path, typer.Option("--output-root")
        ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
        docs_path: Annotated[Path | None, typer.Option("--docs-path")] = None,
        strategy_id: Annotated[str, typer.Option("--strategy-id")] = "equal_risk_qqq_sgov",
        execution_policy_id: Annotated[
            str, typer.Option("--execution-policy-id")
        ] = "monthly_plus_threshold_5pct_v1",
        as_of: Annotated[str | None, typer.Option("--as-of")] = None,
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    ) -> None:
        kwargs: dict[str, object] = {
            "prices_path": prices_path,
            "marketstack_prices_path": marketstack_prices_path,
            "rates_path": rates_path,
            "simple_config_path": simple_config_path,
            "growth_config_path": growth_config_path,
            "controlled_growth_config_path": controlled_growth_config_path,
            "layer1_config_path": layer1_config_path,
            "qqq_plus_config_path": qqq_plus_config_path,
            "policy_registry_path": policy_registry_path,
            "output_root": output_root,
            "strategy_id": strategy_id,
            "execution_policy_id": execution_policy_id,
            **_date_range_kwargs(as_of, start_date, end_date),
        }
        if docs_path is not None:
            kwargs["docs_path"] = docs_path
        payload = _call_builder(builder, kwargs)
        _print_execution_semantics_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _execution_semantics_rebacktest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Annotated[
        Path, typer.Option("--signal-validity-taxonomy")
    ] = DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    event_override_policy_path: Annotated[
        Path, typer.Option("--event-override-policy")
    ] = DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output", "--output-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    strategy: Annotated[
        list[str] | None,
        typer.Option("--strategy", help="Strategy id; may be repeated."),
    ] = None,
    execution_policy_id: Annotated[
        str | None,
        typer.Option("--execution-policy-id"),
    ] = None,
    enable_staleness_filter: Annotated[
        bool,
        typer.Option("--enable-staleness-filter"),
    ] = False,
    stale_action: Annotated[
        str | None,
        typer.Option(
            "--stale-action",
            help=(
                "Override stale action: suppress_rebalance, hold_previous_position, "
                "fallback_to_static_baseline, or no_trade."
            ),
        ),
    ] = None,
    include_repaired_watch_only: Annotated[
        bool,
        typer.Option("--include-repaired-watch-only"),
    ] = False,
    emit_staleness_decomposition: Annotated[
        bool,
        typer.Option("--emit-staleness-decomposition"),
    ] = False,
    emit_lag_decomposition: Annotated[
        bool,
        typer.Option("--emit-lag-decomposition"),
    ] = False,
    staleness_input_summary_path: Annotated[
        Path,
        typer.Option("--staleness-input-summary-path"),
    ] = DEFAULT_SIGNAL_VALIDITY_STALENESS_INPUT_SUMMARY_PATH,
    staleness_repair_matrix_path: Annotated[
        Path,
        typer.Option("--staleness-repair-matrix-path"),
    ] = DEFAULT_STALENESS_REPAIR_MATRIX_YAML_PATH,
    staleness_repair_review_path: Annotated[
        Path,
        typer.Option("--staleness-repair-review-path"),
    ] = DEFAULT_SIGNAL_VALIDITY_STALENESS_REPAIR_REVIEW_PATH,
    enable_event_override: Annotated[
        bool,
        typer.Option("--enable-event-override"),
    ] = False,
    event_override_mode: Annotated[
        str,
        typer.Option("--event-override-mode"),
    ] = EVENT_OVERRIDE_MODE_T_PLUS_1,
    emit_pending_plan_ledger: Annotated[
        bool,
        typer.Option("--emit-pending-plan-ledger"),
    ] = False,
    emit_supersede_log: Annotated[
        bool,
        typer.Option("--emit-supersede-log"),
    ] = False,
    emit_event_override_trace: Annotated[
        bool,
        typer.Option("--emit-event-override-trace"),
    ] = False,
    event_override_survival_matrix_path: Annotated[
        Path,
        typer.Option("--event-override-survival-matrix-path"),
    ] = DEFAULT_EVENT_OVERRIDE_SURVIVAL_MATRIX_YAML_PATH,
    event_override_review_path: Annotated[
        Path,
        typer.Option("--event-override-review-path"),
    ] = DEFAULT_EVENT_OVERRIDE_EXECUTION_SEMANTICS_REVIEW_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        signal_validity_taxonomy_path=signal_validity_taxonomy_path,
        event_override_policy_path=event_override_policy_path,
        output_root=output_root,
        strategy_ids=strategy or list(DEFAULT_EXECUTION_REBACKTEST_STRATEGY_IDS),
        execution_policy_id=execution_policy_id,
        **_date_range_kwargs(as_of, start_date, end_date),
        enable_staleness_filter=enable_staleness_filter,
        stale_action=stale_action,
        include_repaired_watch_only=include_repaired_watch_only,
        emit_staleness_decomposition=emit_staleness_decomposition,
        emit_lag_decomposition=emit_lag_decomposition,
        staleness_input_summary_path=staleness_input_summary_path,
        staleness_repair_matrix_path=staleness_repair_matrix_path,
        staleness_repair_review_path=staleness_repair_review_path,
        enable_event_override=enable_event_override,
        event_override_mode=event_override_mode,
        emit_pending_plan_ledger=emit_pending_plan_ledger,
        emit_supersede_log=emit_supersede_log,
        emit_event_override_trace=emit_event_override_trace,
        event_override_survival_matrix_path=event_override_survival_matrix_path,
        event_override_review_path=event_override_review_path,
    )
    _print_execution_semantics_payload("Execution semantics rebacktest", payload)


def _dynamic_actual_path_owner_review_decision_command(
    output_root: Annotated[
        Path, typer.Option("--source-root", "--output-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    docs_path: Annotated[
        Path, typer.Option("--docs-path")
    ] = DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_DOC_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path")
    ] = DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_YAML_PATH,
) -> None:
    payload = run_dynamic_actual_path_owner_review_decision(
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
    )
    _print_execution_semantics_payload("Dynamic actual-path owner review decision", payload)


def _dynamic_actual_path_policy_sensitivity_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_POLICY_SENSITIVITY_OUTPUT_ROOT,
    docs_path: Annotated[
        Path, typer.Option("--docs-path")
    ] = DEFAULT_DYNAMIC_POLICY_SENSITIVITY_DOC_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path")
    ] = DEFAULT_DYNAMIC_POLICY_SENSITIVITY_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_actual_path_policy_sensitivity_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("Dynamic actual-path policy sensitivity", payload)


def _actual_path_edge_attribution_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    objective_config_path: Annotated[
        Path, typer.Option("--objective-config", "--objectives-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_EDGE_ATTRIBUTION_OUTPUT_ROOT,
    run_id: Annotated[
        str | None, typer.Option("--run-id")
    ] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_actual_path_edge_attribution_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        output_root=output_root,
        run_id=run_id,
        source_root=source_root,
        objective_config_path=objective_config_path,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("Actual-path edge attribution", payload)


def _dynamic_strategy_objective_gate_review_command(
    edge_matrix_path: Annotated[
        Path, typer.Option("--edge-matrix", "--edge-matrix-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    objectives_path: Annotated[
        Path, typer.Option("--objective-config", "--objectives-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    promotion_gate_path: Annotated[
        Path, typer.Option("--gate-config", "--promotion-gate-path")
    ] = DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH,
    docs_path: Annotated[
        Path, typer.Option("--review-path", "--docs-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--matrix-path", "--yaml-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_MATRIX_YAML_PATH,
) -> None:
    payload = run_dynamic_strategy_objective_gate_review(
        edge_matrix_path=edge_matrix_path,
        objectives_path=objectives_path,
        promotion_gate_path=promotion_gate_path,
        docs_path=docs_path,
        yaml_path=yaml_path,
    )
    _print_execution_semantics_payload("Dynamic strategy objective gate review", payload)


def _pit_data_availability_audit_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Annotated[
        Path, typer.Option("--signal-validity-taxonomy")
    ] = DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    event_override_policy_path: Annotated[
        Path, typer.Option("--event-override-policy")
    ] = DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_PIT_AUDIT_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_PIT_DATA_AVAILABILITY_AUDIT_REVIEW_PATH,
    inventory_path: Annotated[
        Path, typer.Option("--inventory-path", "--yaml-path")
    ] = DEFAULT_PIT_DATA_AVAILABILITY_INVENTORY_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_pit_data_availability_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        signal_validity_taxonomy_path=signal_validity_taxonomy_path,
        event_override_policy_path=event_override_policy_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        inventory_path=inventory_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("PIT data availability audit", payload)


def _dynamic_strategy_walk_forward_validation_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    walk_forward_policy_path: Annotated[
        Path, typer.Option("--walk-forward-policy")
    ] = DEFAULT_DYNAMIC_WALK_FORWARD_POLICY_PATH,
    edge_matrix_path: Annotated[
        Path, typer.Option("--edge-matrix", "--edge-matrix-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_WALK_FORWARD_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_walk_forward_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        walk_forward_policy_path=walk_forward_policy_path,
        edge_matrix_path=edge_matrix_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("Dynamic strategy walk-forward validation", payload)


def _event_override_ex_ante_taxonomy_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    event_override_policy_path: Annotated[
        Path, typer.Option("--event-override-policy")
    ] = DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    taxonomy_config_path: Annotated[
        Path, typer.Option("--taxonomy-config", "--taxonomy-policy")
    ] = DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_CONFIG_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_EVENT_TAXONOMY_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--snapshot-path")
    ] = DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_SNAPSHOT_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_event_override_ex_ante_taxonomy_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        event_override_policy_path=event_override_policy_path,
        taxonomy_config_path=taxonomy_config_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload("Event override ex-ante taxonomy review", payload)


def _risk_timing_quality_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    timing_policy_path: Annotated[
        Path, typer.Option("--timing-policy")
    ] = DEFAULT_RISK_TIMING_QUALITY_POLICY_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_TIMING_QUALITY_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_RISK_TIMING_QUALITY_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_RISK_TIMING_QUALITY_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_risk_timing_quality_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        timing_policy_path=timing_policy_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("Risk-off risk-on timing quality review", payload)


def _transaction_cost_cash_yield_audit_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    transaction_cost_model_path: Annotated[
        Path, typer.Option("--transaction-cost-model")
    ] = DEFAULT_TRANSACTION_COST_MODEL_PATH,
    cash_yield_model_path: Annotated[
        Path, typer.Option("--cash-yield-model")
    ] = DEFAULT_CASH_YIELD_MODEL_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_COST_CASH_YIELD_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_TRANSACTION_COST_CASH_YIELD_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_TRANSACTION_COST_CASH_YIELD_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_transaction_cost_cash_yield_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        transaction_cost_model_path=transaction_cost_model_path,
        cash_yield_model_path=cash_yield_model_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("Transaction cost cash yield audit", payload)


def _stress_risk_metrics_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    stress_policy_path: Annotated[
        Path, typer.Option("--stress-policy")
    ] = DEFAULT_STRESS_RISK_METRICS_POLICY_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_STRESS_RISK_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_STRESS_RISK_METRICS_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_STRESS_RISK_METRICS_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_stress_risk_metrics_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        stress_policy_path=stress_policy_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload("Stress risk metrics review", payload)


def _regime_segmentation_baseline_expansion_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    regime_policy_path: Annotated[
        Path, typer.Option("--regime-policy")
    ] = DEFAULT_REGIME_BASELINE_EXPANSION_POLICY_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_REGIME_BASELINE_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_REGIME_BASELINE_EXPANSION_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_REGIME_BASELINE_EXPANSION_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_regime_segmentation_baseline_expansion_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        regime_policy_path=regime_policy_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Regime segmentation baseline expansion review",
        payload,
    )


def _research_artifact_governance_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_ARTIFACT_GOVERNANCE_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_RESEARCH_ARTIFACT_GOVERNANCE_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--snapshot-path")
    ] = DEFAULT_RESEARCH_ARTIFACT_GOVERNANCE_SNAPSHOT_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_research_artifact_governance_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload("Research artifact governance review", payload)


def _dynamic_strategy_execution_cadence_bias_audit_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_DOCS_ROOT,
    strategy: Annotated[
        list[str] | None,
        typer.Option("--strategy", help="Dynamic strategy id; may be repeated."),
    ] = None,
    transaction_cost_bps: Annotated[
        float | None,
        typer.Option("--transaction-cost-bps"),
    ] = None,
    slippage_bps: Annotated[
        float | None,
        typer.Option("--slippage-bps"),
    ] = None,
    turnover_penalty: Annotated[
        float,
        typer.Option("--turnover-penalty"),
    ] = 0.0,
    max_turnover_per_month: Annotated[
        float,
        typer.Option("--max-turnover-per-month"),
    ] = 1.0,
    min_holding_days: Annotated[
        int,
        typer.Option("--min-holding-days"),
    ] = 20,
    cooldown_days: Annotated[
        int,
        typer.Option("--cooldown-days"),
    ] = 20,
    max_single_step_weight_delta: Annotated[
        float,
        typer.Option("--max-single-step-weight-delta"),
    ] = 0.75,
    risk_cap_enabled: Annotated[
        bool,
        typer.Option("--risk-cap-enabled/--risk-cap-disabled"),
    ] = True,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_execution_cadence_bias_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        strategy_ids=strategy,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
        turnover_penalty=turnover_penalty,
        max_turnover_per_month=max_turnover_per_month,
        min_holding_days=min_holding_days,
        cooldown_days=cooldown_days,
        max_single_step_weight_delta=max_single_step_weight_delta,
        risk_cap_enabled=risk_cap_enabled,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy execution cadence bias audit",
        payload,
    )


def _dynamic_strategy_event_driven_retest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_cadence_audit_path: Annotated[
        Path, typer.Option("--source-cadence-audit")
    ] = DEFAULT_SOURCE_CADENCE_AUDIT_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_DOCS_ROOT,
    strategy: Annotated[
        list[str] | None,
        typer.Option("--strategy", help="Dynamic strategy id; may be repeated."),
    ] = None,
    transaction_cost_bps: Annotated[
        float | None,
        typer.Option("--transaction-cost-bps"),
    ] = None,
    slippage_bps: Annotated[
        float | None,
        typer.Option("--slippage-bps"),
    ] = None,
    turnover_penalty: Annotated[
        float,
        typer.Option("--turnover-penalty"),
    ] = 0.0,
    max_turnover_per_month: Annotated[
        float,
        typer.Option("--max-turnover-per-month"),
    ] = 1.0,
    min_holding_days: Annotated[
        int,
        typer.Option("--min-holding-days"),
    ] = 20,
    cooldown_days: Annotated[
        int,
        typer.Option("--cooldown-days"),
    ] = 20,
    max_single_step_weight_delta: Annotated[
        float,
        typer.Option("--max-single-step-weight-delta"),
    ] = 0.75,
    risk_cap_enabled: Annotated[
        bool,
        typer.Option("--risk-cap-enabled/--risk-cap-disabled"),
    ] = True,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_event_driven_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_cadence_audit_path=source_cadence_audit_path,
        output_root=output_root,
        docs_root=docs_root,
        strategy_ids=strategy,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
        turnover_penalty=turnover_penalty,
        max_turnover_per_month=max_turnover_per_month,
        min_holding_days=min_holding_days,
        cooldown_days=cooldown_days,
        max_single_step_weight_delta=max_single_step_weight_delta,
        risk_cap_enabled=risk_cap_enabled,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy event-driven retest",
        payload,
    )


def _dynamic_strategy_cost_turnover_cooldown_sensitivity_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Annotated[
        Path, typer.Option("--source-cadence-matrix")
    ] = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_DOCS_ROOT,
    strategy: Annotated[
        list[str] | None,
        typer.Option("--strategy", help="Dynamic strategy id; may be repeated."),
    ] = None,
    turnover_penalty: Annotated[
        float,
        typer.Option("--turnover-penalty"),
    ] = 0.0,
    risk_cap_enabled: Annotated[
        bool,
        typer.Option("--risk-cap-enabled/--risk-cap-disabled"),
    ] = True,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_cost_turnover_cooldown_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        output_root=output_root,
        docs_root=docs_root,
        strategy_ids=strategy,
        turnover_penalty=turnover_penalty,
        risk_cap_enabled=risk_cap_enabled,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy cost turnover cooldown sensitivity",
        payload,
    )


def _dynamic_strategy_top_candidate_owner_review_gate_command(
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Annotated[
        Path, typer.Option("--source-cadence-matrix")
    ] = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_sensitivity_matrix_path: Annotated[
        Path, typer.Option("--source-sensitivity-matrix")
    ] = DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH,
    source_decision_update_path: Annotated[
        Path, typer.Option("--source-decision-update")
    ] = DEFAULT_SOURCE_DECISION_UPDATE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_top_candidate_owner_review_gate(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_decision_update_path=source_decision_update_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy top candidate owner review gate",
        payload,
    )


def _dynamic_strategy_research_only_shadow_observation_protocol_command(
    source_owner_review_gate_path: Annotated[
        Path, typer.Option("--source-owner-review-gate")
    ] = DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    source_candidate_owner_review_comparison_path: Annotated[
        Path, typer.Option("--source-candidate-owner-review-comparison")
    ] = DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH,
    source_shadow_research_gate_decision_path: Annotated[
        Path, typer.Option("--source-shadow-research-gate-decision")
    ] = DEFAULT_SOURCE_SHADOW_RESEARCH_GATE_DECISION_PATH,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_sensitivity_matrix_path: Annotated[
        Path, typer.Option("--source-sensitivity-matrix")
    ] = DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH,
    source_decision_update_path: Annotated[
        Path, typer.Option("--source-decision-update")
    ] = DEFAULT_SOURCE_DECISION_UPDATE_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Annotated[
        Path, typer.Option("--source-cadence-matrix")
    ] = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_research_only_shadow_observation_protocol(
        source_owner_review_gate_path=source_owner_review_gate_path,
        source_candidate_owner_review_comparison_path=(
            source_candidate_owner_review_comparison_path
        ),
        source_shadow_research_gate_decision_path=(
            source_shadow_research_gate_decision_path
        ),
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_decision_update_path=source_decision_update_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only shadow observation protocol",
        payload,
    )


def _dynamic_strategy_research_only_shadow_observation_dry_run_command(
    source_observation_protocol_path: Annotated[
        Path, typer.Option("--source-observation-protocol")
    ] = DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH,
    source_observation_field_schema_path: Annotated[
        Path, typer.Option("--source-observation-field-schema")
    ] = DEFAULT_SOURCE_OBSERVATION_FIELD_SCHEMA_PATH,
    source_review_thresholds_path: Annotated[
        Path, typer.Option("--source-review-thresholds")
    ] = DEFAULT_SOURCE_REVIEW_THRESHOLDS_PATH,
    source_owner_review_gate_path: Annotated[
        Path, typer.Option("--source-owner-review-gate")
    ] = DEFAULT_OPT_SOURCE_OWNER_GATE,
    source_candidate_owner_review_comparison_path: Annotated[
        Path, typer.Option("--source-candidate-owner-review-comparison")
    ] = DEFAULT_OPT_CANDIDATE_CMP,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_research_only_shadow_observation_dry_run(
        source_observation_protocol_path=source_observation_protocol_path,
        source_observation_field_schema_path=source_observation_field_schema_path,
        source_review_thresholds_path=source_review_thresholds_path,
        source_owner_review_gate_path=source_owner_review_gate_path,
        source_candidate_owner_review_comparison_path=(
            source_candidate_owner_review_comparison_path
        ),
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_event_retest_path=source_event_retest_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only shadow observation dry-run",
        payload,
    )


def _dynamic_strategy_research_only_shadow_observation_replay_validation_command(
    source_dry_run_result_path: Annotated[
        Path, typer.Option("--source-dry-run-result")
    ] = DEFAULT_SOURCE_DRY_RUN_RESULT_PATH,
    source_dry_run_record_path: Annotated[
        Path, typer.Option("--source-dry-run-record")
    ] = DEFAULT_SOURCE_DRY_RUN_RECORD_PATH,
    source_dry_run_no_side_effect_evidence_path: Annotated[
        Path, typer.Option("--source-dry-run-no-side-effect-evidence")
    ] = DEFAULT_SOURCE_DRY_RUN_NO_SIDE_EFFECT_EVIDENCE_PATH,
    source_observation_protocol_path: Annotated[
        Path, typer.Option("--source-observation-protocol")
    ] = DEFAULT_REPLAY_SOURCE_OBSERVATION_PROTOCOL_PATH,
    source_owner_review_gate_path: Annotated[
        Path, typer.Option("--source-owner-review-gate")
    ] = DEFAULT_REPLAY_SOURCE_OWNER_REVIEW_GATE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    replay_count: Annotated[int, typer.Option("--replay-count")] = DEFAULT_REPLAY_COUNT,
) -> None:
    payload = run_dynamic_strategy_research_only_shadow_observation_replay_validation(
        source_dry_run_result_path=source_dry_run_result_path,
        source_dry_run_record_path=source_dry_run_record_path,
        source_dry_run_no_side_effect_evidence_path=(
            source_dry_run_no_side_effect_evidence_path
        ),
        source_observation_protocol_path=source_observation_protocol_path,
        source_owner_review_gate_path=source_owner_review_gate_path,
        output_root=output_root,
        docs_root=docs_root,
        replay_count=replay_count,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only shadow observation replay validation",
        payload,
    )


def _dynamic_strategy_research_only_shadow_observation_owner_review_decision_command(
    source_replay_validation_path: Annotated[
        Path, typer.Option("--source-replay-validation")
    ] = DEFAULT_SOURCE_REPLAY_VALIDATION_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_research_only_shadow_observation_owner_review_decision(
        source_replay_validation_path=source_replay_validation_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only shadow observation owner review decision",
        payload,
    )


def _dynamic_strategy_research_only_observation_log_schema_plan_command(
    source_owner_review_decision_path: Annotated[
        Path, typer.Option("--source-owner-review-decision")
    ] = DEFAULT_SOURCE_OWNER_REVIEW_DECISION_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_research_only_observation_log_schema_plan(
        source_owner_review_decision_path=source_owner_review_decision_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only observation log schema plan",
        payload,
    )


def _dynamic_strategy_research_only_observation_report_dry_run_command(
    source_owner_review_decision_path: Annotated[
        Path, typer.Option("--source-owner-review-decision")
    ] = DEFAULT_SOURCE_2371_OWNER_REVIEW_DECISION_PATH,
    source_log_schema_plan_path: Annotated[
        Path, typer.Option("--source-log-schema-plan")
    ] = DEFAULT_SOURCE_2372_LOG_SCHEMA_PLAN_PATH,
    source_observation_dry_run_result_path: Annotated[
        Path, typer.Option("--source-observation-dry-run-result")
    ] = DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RESULT_PATH,
    source_observation_dry_run_record_path: Annotated[
        Path, typer.Option("--source-observation-dry-run-record")
    ] = DEFAULT_SOURCE_2369_OBSERVATION_DRY_RUN_RECORD_PATH,
    source_replay_validation_path: Annotated[
        Path, typer.Option("--source-replay-validation")
    ] = DEFAULT_SOURCE_2370_REPLAY_VALIDATION_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_research_only_observation_report_dry_run(
        source_owner_review_decision_path=source_owner_review_decision_path,
        source_log_schema_plan_path=source_log_schema_plan_path,
        source_observation_dry_run_result_path=source_observation_dry_run_result_path,
        source_observation_dry_run_record_path=source_observation_dry_run_record_path,
        source_replay_validation_path=source_replay_validation_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only observation report dry-run",
        payload,
    )


def _dynamic_strategy_research_only_observation_owner_reassessment_command(
    source_report_dry_run_path: Annotated[
        Path, typer.Option("--source-report-dry-run")
    ] = DEFAULT_SOURCE_2373_REPORT_DRY_RUN_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_research_only_observation_owner_reassessment(
        source_report_dry_run_path=source_report_dry_run_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research-only observation owner reassessment",
        payload,
    )


def _dynamic_strategy_candidate_optimization_divergence_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Annotated[
        Path, typer.Option("--source-cadence-matrix")
    ] = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_OPT_SOURCE_SENSITIVITY_RESULT,
    source_sensitivity_matrix_path: Annotated[
        Path, typer.Option("--source-sensitivity-matrix")
    ] = DEFAULT_OPT_SOURCE_SENSITIVITY_MATRIX,
    source_decision_update_path: Annotated[
        Path, typer.Option("--source-decision-update")
    ] = DEFAULT_OPT_SOURCE_DECISION_UPDATE,
    source_owner_review_gate_path: Annotated[
        Path, typer.Option("--source-owner-review-gate")
    ] = DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    source_candidate_owner_review_comparison_path: Annotated[
        Path, typer.Option("--source-candidate-owner-review-comparison")
    ] = DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH,
    source_owner_reassessment_path: Annotated[
        Path, typer.Option("--source-owner-reassessment")
    ] = DEFAULT_SOURCE_OWNER_REASSESSMENT_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_candidate_optimization_divergence_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_decision_update_path=source_decision_update_path,
        source_owner_review_gate_path=source_owner_review_gate_path,
        source_candidate_owner_review_comparison_path=(
            source_candidate_owner_review_comparison_path
        ),
        source_owner_reassessment_path=source_owner_reassessment_path,
        output_root=output_root,
        docs_root=docs_root,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy candidate optimization divergence review",
        payload,
    )


def _dynamic_strategy_optimized_candidate_targeted_retest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Annotated[
        Path, typer.Option("--source-cadence-matrix")
    ] = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_TARGETED_RETEST_SOURCE_2366_RESULT,
    source_sensitivity_matrix_path: Annotated[
        Path, typer.Option("--source-sensitivity-matrix")
    ] = DEFAULT_TARGETED_RETEST_SOURCE_2366_MATRIX,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_TARGETED_RETEST_SOURCE_2366_DECISION_UPDATE,
    source_optimization_review_path: Annotated[
        Path, typer.Option("--source-optimization-review")
    ] = DEFAULT_TARGETED_RETEST_SOURCE_2375_RESULT,
    source_optimization_matrix_path: Annotated[
        Path, typer.Option("--source-optimization-matrix")
    ] = DEFAULT_TARGETED_RETEST_SOURCE_2375_MATRIX,
    source_optimization_decision_update_path: Annotated[
        Path, typer.Option("--source-optimization-decision-update")
    ] = DEFAULT_TARGETED_RETEST_SOURCE_2375_DECISION_UPDATE,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_optimized_candidate_targeted_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_matrix_path=source_optimization_matrix_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy optimized candidate targeted retest",
        payload,
    )


def _dynamic_strategy_targeted_retest_owner_review_decision_command(
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_2377_SOURCE_EVENT_RETEST,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2377_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2377_SOURCE_SENSITIVITY_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2377_SENS_DECISION,
    source_optimization_review_path: Annotated[
        Path, typer.Option("--source-optimization-review")
    ] = DEFAULT_2377_SOURCE_OPTIMIZATION_REVIEW,
    source_optimization_decision_update_path: Annotated[
        Path, typer.Option("--source-optimization-decision-update")
    ] = DEFAULT_2377_OPT_DECISION,
    source_targeted_retest_path: Annotated[
        Path, typer.Option("--source-targeted-retest")
    ] = DEFAULT_2377_SOURCE_TARGETED_RETEST,
    source_targeted_decision_update_path: Annotated[
        Path, typer.Option("--source-targeted-decision-update")
    ] = DEFAULT_2377_SOURCE_TARGETED_DECISION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_targeted_retest_owner_review_decision(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy targeted retest owner review decision",
        payload,
    )


def _dynamic_strategy_slice_robustness_return_gap_optimization_plan_command(
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_2378_SOURCE_EVENT_RETEST,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2378_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2378_SOURCE_SENSITIVITY_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2378_SENS_DECISION,
    source_optimization_review_path: Annotated[
        Path, typer.Option("--source-optimization-review")
    ] = DEFAULT_2378_SOURCE_OPTIMIZATION_REVIEW,
    source_optimization_decision_update_path: Annotated[
        Path, typer.Option("--source-optimization-decision-update")
    ] = DEFAULT_2378_OPT_DECISION,
    source_targeted_retest_path: Annotated[
        Path, typer.Option("--source-targeted-retest")
    ] = DEFAULT_2378_SOURCE_TARGETED_RETEST,
    source_targeted_decision_update_path: Annotated[
        Path, typer.Option("--source-targeted-decision-update")
    ] = DEFAULT_2378_SOURCE_TARGETED_DECISION,
    source_owner_review_decision_path: Annotated[
        Path, typer.Option("--source-owner-review-decision")
    ] = DEFAULT_2378_SOURCE_OWNER_DECISION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_slice_robustness_return_gap_optimization_plan(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        source_owner_review_decision_path=source_owner_review_decision_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy slice robustness return gap optimization plan",
        payload,
    )


def _dynamic_strategy_slice_robustness_optimized_variant_retest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = DEFAULT_2378_SOURCE_EVENT_RETEST,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2378_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2379_SOURCE_SENSITIVITY_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2379_SENS_DECISION,
    source_optimization_review_path: Annotated[
        Path, typer.Option("--source-optimization-review")
    ] = DEFAULT_2379_SOURCE_OPTIMIZATION_REVIEW,
    source_optimization_decision_update_path: Annotated[
        Path, typer.Option("--source-optimization-decision-update")
    ] = DEFAULT_2379_OPT_DECISION,
    source_targeted_retest_path: Annotated[
        Path, typer.Option("--source-targeted-retest")
    ] = DEFAULT_2379_SOURCE_TARGETED_RETEST,
    source_targeted_decision_update_path: Annotated[
        Path, typer.Option("--source-targeted-decision-update")
    ] = DEFAULT_2379_SOURCE_TARGETED_DECISION,
    source_optimization_plan_path: Annotated[
        Path, typer.Option("--source-optimization-plan")
    ] = DEFAULT_2379_SOURCE_OPTIMIZATION_PLAN,
    source_variant_evaluation_plan_path: Annotated[
        Path, typer.Option("--source-variant-evaluation-plan")
    ] = DEFAULT_2379_SOURCE_VARIANT_EVALUATION_PLAN,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_slice_robustness_optimized_variant_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        source_optimization_plan_path=source_optimization_plan_path,
        source_variant_evaluation_plan_path=source_variant_evaluation_plan_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy slice robustness optimized variant retest",
        payload,
    )


def _dynamic_strategy_optimized_variant_owner_review_decision_command(
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2380_SOURCE_VARIANT_RETEST,
    source_variant_decision_update_path: Annotated[
        Path, typer.Option("--source-variant-decision-update")
    ] = DEFAULT_2380_SOURCE_VARIANT_DECISION,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2380_SOURCE_VARIANT_RANKING,
    source_optimization_plan_path: Annotated[
        Path, typer.Option("--source-optimization-plan")
    ] = DEFAULT_2380_SOURCE_OPTIMIZATION_PLAN,
    source_variant_evaluation_plan_path: Annotated[
        Path, typer.Option("--source-variant-evaluation-plan")
    ] = DEFAULT_2380_SOURCE_VARIANT_EVALUATION_PLAN,
    source_targeted_retest_path: Annotated[
        Path, typer.Option("--source-targeted-retest")
    ] = DEFAULT_2380_SOURCE_TARGETED_RETEST,
    source_targeted_decision_update_path: Annotated[
        Path, typer.Option("--source-targeted-decision-update")
    ] = DEFAULT_2380_SOURCE_TARGETED_DECISION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_optimized_variant_owner_review_decision(
        source_variant_retest_path=source_variant_retest_path,
        source_variant_decision_update_path=source_variant_decision_update_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_optimization_plan_path=source_optimization_plan_path,
        source_variant_evaluation_plan_path=source_variant_evaluation_plan_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy optimized variant owner review decision",
        payload,
    )


def _dynamic_strategy_optimization_plateau_next_candidate_decision_command(
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2381_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2381_SOURCE_SENS_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2381_SOURCE_SENS_DECISION,
    source_targeted_retest_path: Annotated[
        Path, typer.Option("--source-targeted-retest")
    ] = DEFAULT_2381_SOURCE_TARGETED_RETEST,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2381_SOURCE_VARIANT_RETEST,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2381_SOURCE_VARIANT_RANKING,
    source_owner_review_path: Annotated[
        Path, typer.Option("--source-owner-review")
    ] = DEFAULT_2381_SOURCE_OWNER_REVIEW,
    source_observation_rejection_path: Annotated[
        Path, typer.Option("--source-observation-rejection")
    ] = DEFAULT_2381_SOURCE_OBSERVATION_REJECTION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_optimization_plateau_next_candidate_decision(
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_owner_review_path=source_owner_review_path,
        source_observation_rejection_path=source_observation_rejection_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy optimization plateau next candidate decision",
        payload,
    )


def _dynamic_strategy_ranking_top_guarded_turnover_retest_plan_command(
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2382_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2382_SOURCE_SENS_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2382_SOURCE_SENS_DECISION,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2382_SOURCE_VARIANT_RETEST,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2382_SOURCE_VARIANT_RANKING,
    source_owner_review_path: Annotated[
        Path, typer.Option("--source-owner-review")
    ] = DEFAULT_2382_SOURCE_OWNER_REVIEW,
    source_observation_rejection_path: Annotated[
        Path, typer.Option("--source-observation-rejection")
    ] = DEFAULT_2382_SOURCE_OBSERVATION_REJECTION,
    source_plateau_decision_path: Annotated[
        Path, typer.Option("--source-plateau-decision")
    ] = DEFAULT_2382_SOURCE_PLATEAU_DECISION,
    source_next_direction_path: Annotated[
        Path, typer.Option("--source-next-direction")
    ] = DEFAULT_2382_SOURCE_NEXT_DIRECTION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_ranking_top_guarded_turnover_retest_plan(
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_owner_review_path=source_owner_review_path,
        source_observation_rejection_path=source_observation_rejection_path,
        source_plateau_decision_path=source_plateau_decision_path,
        source_next_direction_path=source_next_direction_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy ranking top guarded turnover retest plan",
        payload,
    )


def _dynamic_strategy_ranking_top_guarded_variant_retest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_retest_plan_path: Annotated[
        Path, typer.Option("--source-retest-plan")
    ] = DEFAULT_2383_SOURCE_RETEST_PLAN,
    source_guarded_variant_plan_path: Annotated[
        Path, typer.Option("--source-guarded-variant-plan")
    ] = DEFAULT_2383_SOURCE_GUARDED_PLAN,
    source_variant_evaluation_plan_path: Annotated[
        Path, typer.Option("--source-variant-evaluation-plan")
    ] = DEFAULT_2383_SOURCE_EVALUATION_PLAN,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2383_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2383_SOURCE_SENS_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2383_SOURCE_SENS_DECISION,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2383_SOURCE_VARIANT_RETEST,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2383_SOURCE_VARIANT_RANKING,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_ranking_top_guarded_variant_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_retest_plan_path=source_retest_plan_path,
        source_guarded_variant_plan_path=source_guarded_variant_plan_path,
        source_variant_evaluation_plan_path=source_variant_evaluation_plan_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        output_root=output_root,
        docs_root=docs_root,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy ranking top guarded variant retest",
        payload,
    )


def _dynamic_strategy_guarded_variant_owner_review_decision_command(
    source_guarded_variant_retest_path: Annotated[
        Path, typer.Option("--source-guarded-variant-retest")
    ] = DEFAULT_2384_SOURCE_GUARDED_RETEST,
    source_guarded_decision_update_path: Annotated[
        Path, typer.Option("--source-guarded-decision-update")
    ] = DEFAULT_2384_SOURCE_GUARDED_DECISION,
    source_guarded_variant_ranking_path: Annotated[
        Path, typer.Option("--source-guarded-variant-ranking")
    ] = DEFAULT_2384_SOURCE_GUARDED_RANKING,
    source_retest_plan_path: Annotated[
        Path, typer.Option("--source-retest-plan")
    ] = DEFAULT_2384_SOURCE_RETEST_PLAN,
    source_guarded_variant_plan_path: Annotated[
        Path, typer.Option("--source-guarded-variant-plan")
    ] = DEFAULT_2384_SOURCE_GUARDED_PLAN,
    source_plateau_decision_path: Annotated[
        Path, typer.Option("--source-plateau-decision")
    ] = DEFAULT_2384_SOURCE_PLATEAU_DECISION,
    source_next_direction_path: Annotated[
        Path, typer.Option("--source-next-direction")
    ] = DEFAULT_2384_SOURCE_NEXT_DIRECTION,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2384_SOURCE_VARIANT_RETEST,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2384_SOURCE_VARIANT_RANKING,
    source_owner_review_path: Annotated[
        Path, typer.Option("--source-owner-review")
    ] = DEFAULT_2384_SOURCE_OWNER_REVIEW,
    source_observation_rejection_path: Annotated[
        Path, typer.Option("--source-observation-rejection")
    ] = DEFAULT_2384_SOURCE_OBSERVATION_REJECTION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_guarded_variant_owner_review_decision(
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_retest_plan_path=source_retest_plan_path,
        source_guarded_variant_plan_path=source_guarded_variant_plan_path,
        source_plateau_decision_path=source_plateau_decision_path,
        source_next_direction_path=source_next_direction_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_owner_review_path=source_owner_review_path,
        source_observation_rejection_path=source_observation_rejection_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy guarded variant owner review decision",
        payload,
    )


def _dynamic_strategy_candidate_pool_expansion_plan_command(
    source_owner_review_path: Annotated[
        Path, typer.Option("--source-owner-review")
    ] = DEFAULT_2385_SOURCE_OWNER_REVIEW,
    source_next_direction_path: Annotated[
        Path, typer.Option("--source-next-direction")
    ] = DEFAULT_2385_SOURCE_NEXT_DIRECTION,
    source_guarded_variant_retest_path: Annotated[
        Path, typer.Option("--source-guarded-variant-retest")
    ] = DEFAULT_2385_SOURCE_GUARDED_RETEST,
    source_guarded_variant_ranking_path: Annotated[
        Path, typer.Option("--source-guarded-variant-ranking")
    ] = DEFAULT_2385_SOURCE_GUARDED_RANKING,
    source_guarded_decision_update_path: Annotated[
        Path, typer.Option("--source-guarded-decision-update")
    ] = DEFAULT_2385_SOURCE_GUARDED_DECISION,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2385_SOURCE_VARIANT_RETEST,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2385_SOURCE_VARIANT_RANKING,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2385_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2385_SOURCE_SENS_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2385_SOURCE_SENS_DECISION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_candidate_pool_expansion_plan(
        source_owner_review_path=source_owner_review_path,
        source_next_direction_path=source_next_direction_path,
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy candidate pool expansion plan",
        payload,
    )


def _dynamic_strategy_expanded_candidate_pool_retest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_candidate_pool_plan_path: Annotated[
        Path, typer.Option("--source-candidate-pool-plan")
    ] = DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH,
    source_owner_review_path: Annotated[
        Path, typer.Option("--source-owner-review")
    ] = DEFAULT_2386_SOURCE_OWNER_REVIEW,
    source_guarded_variant_retest_path: Annotated[
        Path, typer.Option("--source-guarded-variant-retest")
    ] = DEFAULT_2386_SOURCE_GUARDED_RETEST,
    source_guarded_variant_ranking_path: Annotated[
        Path, typer.Option("--source-guarded-variant-ranking")
    ] = DEFAULT_2386_SOURCE_GUARDED_RANKING,
    source_guarded_decision_update_path: Annotated[
        Path, typer.Option("--source-guarded-decision-update")
    ] = DEFAULT_2386_SOURCE_GUARDED_DECISION,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = DEFAULT_2386_SOURCE_VARIANT_RETEST,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = DEFAULT_2386_SOURCE_VARIANT_RANKING,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2386_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2386_SOURCE_SENS_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2386_SOURCE_SENS_DECISION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_expanded_candidate_pool_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_candidate_pool_expansion_plan_path=source_candidate_pool_plan_path,
        source_owner_review_path=source_owner_review_path,
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy expanded candidate pool retest",
        payload,
    )


def _dynamic_strategy_observation_gate_threshold_calibration_review_command(
    source_expanded_candidate_retest_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest")
    ] = DEFAULT_2387_SOURCE_RETEST,
    source_expanded_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking")
    ] = DEFAULT_2387_SOURCE_RANKING,
    source_signal_family_screening_path: Annotated[
        Path, typer.Option("--source-signal-family-screening")
    ] = DEFAULT_2387_SOURCE_SIGNAL_FAMILY,
    source_decision_update_path: Annotated[
        Path, typer.Option("--source-decision-update")
    ] = DEFAULT_2387_SOURCE_DECISION_UPDATE,
    source_candidate_pool_plan_path: Annotated[
        Path, typer.Option("--source-candidate-pool-plan")
    ] = DEFAULT_2387_SOURCE_POOL_PLAN,
    source_owner_review_path: Annotated[
        Path, typer.Option("--source-owner-review")
    ] = DEFAULT_2387_SOURCE_OWNER_REVIEW,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = DEFAULT_2387_SOURCE_CANDIDATE_RANKING,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = DEFAULT_2387_SOURCE_SENS_RESULT,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = DEFAULT_2387_SOURCE_SENS_DECISION,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_dynamic_strategy_observation_gate_threshold_calibration_review(
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_signal_family_screening_path=source_signal_family_screening_path,
        source_decision_update_path=source_decision_update_path,
        source_candidate_pool_plan_path=source_candidate_pool_plan_path,
        source_owner_review_path=source_owner_review_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy observation gate threshold calibration review",
        payload,
    )


def _dynamic_strategy_research_filter_threshold_methodology_review_command(
    source_cadence_audit_path: Annotated[
        Path, typer.Option("--source-cadence-audit")
    ] = m2388.DEFAULT_SOURCE_2364_CADENCE_AUDIT_PATH,
    source_event_retest_path: Annotated[
        Path, typer.Option("--source-event-retest")
    ] = m2388.DEFAULT_SOURCE_2365_EVENT_RETEST_PATH,
    source_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-candidate-ranking")
    ] = m2388.DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Annotated[
        Path, typer.Option("--source-sensitivity-result")
    ] = m2388.DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Annotated[
        Path, typer.Option("--source-sensitivity-decision-update")
    ] = m2388.DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH,
    source_divergence_review_path: Annotated[
        Path, typer.Option("--source-divergence-review")
    ] = m2388.DEFAULT_SOURCE_2375_DIVERGENCE_REVIEW_PATH,
    source_divergence_decision_update_path: Annotated[
        Path, typer.Option("--source-divergence-decision-update")
    ] = m2388.DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH,
    source_targeted_retest_path: Annotated[
        Path, typer.Option("--source-targeted-retest")
    ] = m2388.DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_targeted_decision_update_path: Annotated[
        Path, typer.Option("--source-targeted-decision-update")
    ] = m2388.DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH,
    source_variant_retest_path: Annotated[
        Path, typer.Option("--source-variant-retest")
    ] = m2388.DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Annotated[
        Path, typer.Option("--source-optimized-variant-ranking")
    ] = m2388.DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH,
    source_variant_decision_update_path: Annotated[
        Path, typer.Option("--source-variant-decision-update")
    ] = m2388.DEFAULT_SOURCE_2379_DECISION_UPDATE_PATH,
    source_guarded_variant_retest_path: Annotated[
        Path, typer.Option("--source-guarded-variant-retest")
    ] = m2388.DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH,
    source_guarded_variant_ranking_path: Annotated[
        Path, typer.Option("--source-guarded-variant-ranking")
    ] = m2388.DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH,
    source_guarded_decision_update_path: Annotated[
        Path, typer.Option("--source-guarded-decision-update")
    ] = m2388.DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH,
    source_expanded_candidate_retest_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest")
    ] = m2388.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_expanded_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking")
    ] = m2388.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH,
    source_expanded_decision_update_path: Annotated[
        Path, typer.Option("--source-expanded-decision-update")
    ] = m2388.DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH,
    source_gate_calibration_review_path: Annotated[
        Path, typer.Option("--source-gate-calibration-review")
    ] = m2388.DEFAULT_SOURCE_2387_GATE_CALIBRATION_REVIEW_PATH,
    source_gate_policy_review_path: Annotated[
        Path, typer.Option("--source-gate-policy-review")
    ] = m2388.DEFAULT_SOURCE_2387_GATE_POLICY_REVIEW_PATH,
    source_candidate_reclassification_preview_path: Annotated[
        Path, typer.Option("--source-candidate-reclassification-preview")
    ] = m2388.DEFAULT_SOURCE_2387_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH,
    source_recommended_policy_update_path: Annotated[
        Path, typer.Option("--source-recommended-policy-update")
    ] = m2388.DEFAULT_SOURCE_2387_RECOMMENDED_POLICY_UPDATE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2388.DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2388.DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2388.run_dynamic_strategy_research_filter_threshold_methodology_review(
        source_cadence_audit_path=source_cadence_audit_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_divergence_review_path=source_divergence_review_path,
        source_divergence_decision_update_path=source_divergence_decision_update_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_variant_decision_update_path=source_variant_decision_update_path,
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_expanded_decision_update_path=source_expanded_decision_update_path,
        source_gate_calibration_review_path=source_gate_calibration_review_path,
        source_gate_policy_review_path=source_gate_policy_review_path,
        source_candidate_reclassification_preview_path=(
            source_candidate_reclassification_preview_path
        ),
        source_recommended_policy_update_path=source_recommended_policy_update_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy research filter threshold methodology review",
        payload,
    )


def _dynamic_strategy_calibrated_gate_owner_review_decision_command(
    source_expanded_candidate_retest_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest")
    ] = m2389.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_expanded_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking")
    ] = m2389.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH,
    source_expanded_decision_update_path: Annotated[
        Path, typer.Option("--source-expanded-decision-update")
    ] = m2389.DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH,
    source_gate_calibration_review_path: Annotated[
        Path, typer.Option("--source-gate-calibration-review")
    ] = m2389.DEFAULT_SOURCE_2387_GATE_CALIBRATION_REVIEW_PATH,
    source_gate_policy_review_path: Annotated[
        Path, typer.Option("--source-gate-policy-review")
    ] = m2389.DEFAULT_SOURCE_2387_GATE_POLICY_REVIEW_PATH,
    source_candidate_reclassification_preview_path: Annotated[
        Path, typer.Option("--source-candidate-reclassification-preview")
    ] = m2389.DEFAULT_SOURCE_2387_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH,
    source_recommended_policy_update_path: Annotated[
        Path, typer.Option("--source-recommended-policy-update")
    ] = m2389.DEFAULT_SOURCE_2387_RECOMMENDED_POLICY_UPDATE_PATH,
    source_threshold_methodology_review_path: Annotated[
        Path, typer.Option("--source-threshold-methodology-review")
    ] = m2389.DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH,
    source_threshold_inventory_path: Annotated[
        Path, typer.Option("--source-threshold-inventory")
    ] = m2389.DEFAULT_SOURCE_2388_THRESHOLD_INVENTORY_PATH,
    source_gate_taxonomy_path: Annotated[
        Path, typer.Option("--source-gate-taxonomy")
    ] = m2389.DEFAULT_SOURCE_2388_GATE_TAXONOMY_PATH,
    source_candidate_threshold_outcome_matrix_path: Annotated[
        Path, typer.Option("--source-candidate-threshold-outcome-matrix")
    ] = m2389.DEFAULT_SOURCE_2388_CANDIDATE_THRESHOLD_OUTCOME_MATRIX_PATH,
    source_recommended_gate_policy_proposal_path: Annotated[
        Path, typer.Option("--source-recommended-gate-policy-proposal")
    ] = m2389.DEFAULT_SOURCE_2388_RECOMMENDED_GATE_POLICY_PROPOSAL_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2389.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2389.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2389.run_dynamic_strategy_calibrated_gate_owner_review_decision(
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_expanded_decision_update_path=source_expanded_decision_update_path,
        source_gate_calibration_review_path=source_gate_calibration_review_path,
        source_gate_policy_review_path=source_gate_policy_review_path,
        source_candidate_reclassification_preview_path=(
            source_candidate_reclassification_preview_path
        ),
        source_recommended_policy_update_path=source_recommended_policy_update_path,
        source_threshold_methodology_review_path=source_threshold_methodology_review_path,
        source_threshold_inventory_path=source_threshold_inventory_path,
        source_gate_taxonomy_path=source_gate_taxonomy_path,
        source_candidate_threshold_outcome_matrix_path=(
            source_candidate_threshold_outcome_matrix_path
        ),
        source_recommended_gate_policy_proposal_path=(
            source_recommended_gate_policy_proposal_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy calibrated gate owner review decision",
        payload,
    )


def _dynamic_strategy_calibrated_gate_candidate_reclassification_command(
    source_candidate_ranking_2365_path: Annotated[
        Path, typer.Option("--source-candidate-ranking-2365")
    ] = m2390.DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_2366_path: Annotated[
        Path, typer.Option("--source-sensitivity-result-2366")
    ] = m2390.DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_expanded_candidate_retest_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest")
    ] = m2390.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_expanded_candidate_ranking_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking")
    ] = m2390.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH,
    source_expanded_decision_update_path: Annotated[
        Path, typer.Option("--source-expanded-decision-update")
    ] = m2390.DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH,
    source_threshold_methodology_review_path: Annotated[
        Path, typer.Option("--source-threshold-methodology-review")
    ] = m2390.DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH,
    source_candidate_threshold_outcome_matrix_path: Annotated[
        Path, typer.Option("--source-candidate-threshold-outcome-matrix")
    ] = m2390.DEFAULT_SOURCE_2388_CANDIDATE_THRESHOLD_OUTCOME_MATRIX_PATH,
    source_recommended_gate_policy_proposal_path: Annotated[
        Path, typer.Option("--source-recommended-gate-policy-proposal")
    ] = m2390.DEFAULT_SOURCE_2388_RECOMMENDED_GATE_POLICY_PROPOSAL_PATH,
    source_owner_review_decision_path: Annotated[
        Path, typer.Option("--source-owner-review-decision")
    ] = m2390.DEFAULT_SOURCE_2389_OWNER_REVIEW_DECISION_PATH,
    source_calibrated_gate_adoption_record_path: Annotated[
        Path, typer.Option("--source-calibrated-gate-adoption-record")
    ] = m2390.DEFAULT_SOURCE_2389_CALIBRATED_GATE_ADOPTION_RECORD_PATH,
    source_non_approval_record_path: Annotated[
        Path, typer.Option("--source-non-approval-record")
    ] = m2390.DEFAULT_SOURCE_2389_NON_APPROVAL_RECORD_PATH,
    source_next_reclassification_route_path: Annotated[
        Path, typer.Option("--source-next-reclassification-route")
    ] = m2390.DEFAULT_SOURCE_2389_NEXT_RECLASSIFICATION_ROUTE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2390.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2390.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2390.run_dynamic_strategy_calibrated_gate_candidate_reclassification(
        source_candidate_ranking_2365_path=source_candidate_ranking_2365_path,
        source_sensitivity_result_2366_path=source_sensitivity_result_2366_path,
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_expanded_decision_update_path=source_expanded_decision_update_path,
        source_threshold_methodology_review_path=source_threshold_methodology_review_path,
        source_candidate_threshold_outcome_matrix_path=(
            source_candidate_threshold_outcome_matrix_path
        ),
        source_recommended_gate_policy_proposal_path=(
            source_recommended_gate_policy_proposal_path
        ),
        source_owner_review_decision_path=source_owner_review_decision_path,
        source_calibrated_gate_adoption_record_path=(
            source_calibrated_gate_adoption_record_path
        ),
        source_non_approval_record_path=source_non_approval_record_path,
        source_next_reclassification_route_path=source_next_reclassification_route_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy calibrated gate candidate reclassification",
        payload,
    )


def _dynamic_strategy_calibrated_gate_candidate_owner_review_decision_command(
    source_expanded_candidate_retest_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest-2386")
    ] = m2391.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_threshold_methodology_review_2388_path: Annotated[
        Path, typer.Option("--source-threshold-methodology-review-2388")
    ] = m2391.DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH,
    source_owner_review_decision_2389_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2389")
    ] = m2391.DEFAULT_SOURCE_2389_OWNER_REVIEW_DECISION_PATH,
    source_reclassification_result_2390_path: Annotated[
        Path, typer.Option("--source-reclassification-result-2390")
    ] = m2391.DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH,
    source_candidate_reclassification_preview_2390_path: Annotated[
        Path, typer.Option("--source-candidate-reclassification-preview-2390")
    ] = m2391.DEFAULT_SOURCE_2390_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH,
    source_component_attribution_review_2390_path: Annotated[
        Path, typer.Option("--source-component-attribution-review-2390")
    ] = m2391.DEFAULT_SOURCE_2390_COMPONENT_ATTRIBUTION_REVIEW_PATH,
    source_owner_review_recommendation_2390_path: Annotated[
        Path, typer.Option("--source-owner-review-recommendation-2390")
    ] = m2391.DEFAULT_SOURCE_2390_OWNER_REVIEW_RECOMMENDATION_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2391.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2391.DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2391.run_dynamic_strategy_calibrated_gate_candidate_owner_review_decision(
            source_expanded_candidate_retest_2386_path=(
                source_expanded_candidate_retest_2386_path
            ),
            source_threshold_methodology_review_2388_path=(
                source_threshold_methodology_review_2388_path
            ),
            source_owner_review_decision_2389_path=(
                source_owner_review_decision_2389_path
            ),
            source_reclassification_result_2390_path=(
                source_reclassification_result_2390_path
            ),
            source_candidate_reclassification_preview_2390_path=(
                source_candidate_reclassification_preview_2390_path
            ),
            source_component_attribution_review_2390_path=(
                source_component_attribution_review_2390_path
            ),
            source_owner_review_recommendation_2390_path=(
                source_owner_review_recommendation_2390_path
            ),
            output_root=output_root,
            docs_root=docs_root,
            **_as_of_kwargs(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy calibrated gate candidate owner review decision",
        payload,
    )


def _dynamic_strategy_component_attribution_gate_evidence_plan_command(
    source_candidate_ranking_2365_path: Annotated[
        Path, typer.Option("--source-candidate-ranking-2365")
    ] = m2392.DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_2366_path: Annotated[
        Path, typer.Option("--source-sensitivity-result-2366")
    ] = m2392.DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_expanded_candidate_retest_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest-2386")
    ] = m2392.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_expanded_candidate_ranking_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking-2386")
    ] = m2392.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH,
    source_reclassification_result_2390_path: Annotated[
        Path, typer.Option("--source-reclassification-result-2390")
    ] = m2392.DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH,
    source_component_attribution_review_2390_path: Annotated[
        Path, typer.Option("--source-component-attribution-review-2390")
    ] = m2392.DEFAULT_SOURCE_2390_COMPONENT_ATTRIBUTION_REVIEW_PATH,
    source_candidate_reclassification_preview_2390_path: Annotated[
        Path, typer.Option("--source-candidate-reclassification-preview-2390")
    ] = m2392.DEFAULT_SOURCE_2390_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH,
    source_owner_review_decision_2391_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2391")
    ] = m2392.DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH,
    source_candidate_owner_review_record_2391_path: Annotated[
        Path, typer.Option("--source-candidate-owner-review-record-2391")
    ] = m2392.DEFAULT_SOURCE_2391_CANDIDATE_OWNER_REVIEW_RECORD_PATH,
    source_observation_non_approval_record_2391_path: Annotated[
        Path, typer.Option("--source-observation-non-approval-record-2391")
    ] = m2392.DEFAULT_SOURCE_2391_OBSERVATION_NON_APPROVAL_RECORD_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2392.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2392.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2392.run_dynamic_strategy_component_attribution_gate_evidence_plan(
        source_candidate_ranking_2365_path=source_candidate_ranking_2365_path,
        source_sensitivity_result_2366_path=source_sensitivity_result_2366_path,
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_expanded_candidate_ranking_2386_path=(
            source_expanded_candidate_ranking_2386_path
        ),
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_component_attribution_review_2390_path=(
            source_component_attribution_review_2390_path
        ),
        source_candidate_reclassification_preview_2390_path=(
            source_candidate_reclassification_preview_2390_path
        ),
        source_owner_review_decision_2391_path=source_owner_review_decision_2391_path,
        source_candidate_owner_review_record_2391_path=(
            source_candidate_owner_review_record_2391_path
        ),
        source_observation_non_approval_record_2391_path=(
            source_observation_non_approval_record_2391_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy component attribution gate evidence plan",
        payload,
    )


def _dynamic_strategy_component_attribution_targeted_ablation_retest_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2393.DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = m2393.DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = m2393.DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config-path")
    ] = m2393.DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry-path")
    ] = m2393.DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_candidate_ranking_2365_path: Annotated[
        Path, typer.Option("--source-candidate-ranking-2365")
    ] = m2393.DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_2366_path: Annotated[
        Path, typer.Option("--source-sensitivity-result-2366")
    ] = m2393.DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_expanded_candidate_retest_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest-2386")
    ] = m2393.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_expanded_candidate_ranking_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking-2386")
    ] = m2393.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH,
    source_reclassification_result_2390_path: Annotated[
        Path, typer.Option("--source-reclassification-result-2390")
    ] = m2393.DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH,
    source_owner_review_decision_2391_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2391")
    ] = m2393.DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH,
    source_component_attribution_plan_2392_path: Annotated[
        Path, typer.Option("--source-component-attribution-plan-2392")
    ] = m2393.DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = m2393.run_dynamic_strategy_component_attribution_targeted_ablation_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_candidate_ranking_2365_path=source_candidate_ranking_2365_path,
        source_sensitivity_result_2366_path=source_sensitivity_result_2366_path,
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_expanded_candidate_ranking_2386_path=(
            source_expanded_candidate_ranking_2386_path
        ),
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_owner_review_decision_2391_path=(
            source_owner_review_decision_2391_path
        ),
        source_component_attribution_plan_2392_path=(
            source_component_attribution_plan_2392_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy component attribution targeted ablation retest",
        payload,
    )


def _dynamic_strategy_component_ablation_owner_review_decision_command(
    source_owner_review_decision_2391_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2391")
    ] = m2394.DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH,
    source_component_attribution_plan_2392_path: Annotated[
        Path, typer.Option("--source-component-attribution-plan-2392")
    ] = m2394.DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH,
    source_ablation_retest_result_2393_path: Annotated[
        Path, typer.Option("--source-ablation-retest-result-2393")
    ] = m2394.DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH,
    source_component_attribution_matrix_2393_path: Annotated[
        Path, typer.Option("--source-component-attribution-matrix-2393")
    ] = m2394.DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH,
    source_reusable_component_decision_2393_path: Annotated[
        Path, typer.Option("--source-reusable-component-decision-2393")
    ] = m2394.DEFAULT_SOURCE_2393_REUSABLE_COMPONENT_DECISION_PATH,
    source_decision_update_2393_path: Annotated[
        Path, typer.Option("--source-decision-update-2393")
    ] = m2394.DEFAULT_SOURCE_2393_DECISION_UPDATE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2394.run_dynamic_strategy_component_ablation_owner_review_decision(
        source_owner_review_decision_2391_path=source_owner_review_decision_2391_path,
        source_component_attribution_plan_2392_path=(
            source_component_attribution_plan_2392_path
        ),
        source_ablation_retest_result_2393_path=(
            source_ablation_retest_result_2393_path
        ),
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        source_reusable_component_decision_2393_path=(
            source_reusable_component_decision_2393_path
        ),
        source_decision_update_2393_path=source_decision_update_2393_path,
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy component ablation owner review decision",
        payload,
    )


def _dynamic_strategy_component_recombination_candidate_plan_command(
    source_reclassification_result_2390_path: Annotated[
        Path, typer.Option("--source-reclassification-result-2390")
    ] = m2395.DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH,
    source_owner_review_decision_2391_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2391")
    ] = m2395.DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH,
    source_component_attribution_plan_2392_path: Annotated[
        Path, typer.Option("--source-component-attribution-plan-2392")
    ] = m2395.DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH,
    source_ablation_retest_result_2393_path: Annotated[
        Path, typer.Option("--source-ablation-retest-result-2393")
    ] = m2395.DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH,
    source_component_attribution_matrix_2393_path: Annotated[
        Path, typer.Option("--source-component-attribution-matrix-2393")
    ] = m2395.DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH,
    source_reusable_component_decision_2393_path: Annotated[
        Path, typer.Option("--source-reusable-component-decision-2393")
    ] = m2395.DEFAULT_SOURCE_2393_REUSABLE_COMPONENT_DECISION_PATH,
    source_owner_review_decision_2394_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2394")
    ] = m2395.DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH,
    source_component_recombination_decision_2394_path: Annotated[
        Path, typer.Option("--source-component-recombination-decision-2394")
    ] = m2395.DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH,
    source_recombination_principles_2394_path: Annotated[
        Path, typer.Option("--source-recombination-principles-2394")
    ] = m2395.DEFAULT_SOURCE_2394_RECOMBINATION_PRINCIPLES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2395.run_dynamic_strategy_component_recombination_candidate_plan(
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_owner_review_decision_2391_path=source_owner_review_decision_2391_path,
        source_component_attribution_plan_2392_path=(
            source_component_attribution_plan_2392_path
        ),
        source_ablation_retest_result_2393_path=(
            source_ablation_retest_result_2393_path
        ),
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        source_reusable_component_decision_2393_path=(
            source_reusable_component_decision_2393_path
        ),
        source_owner_review_decision_2394_path=source_owner_review_decision_2394_path,
        source_component_recombination_decision_2394_path=(
            source_component_recombination_decision_2394_path
        ),
        source_recombination_principles_2394_path=(
            source_recombination_principles_2394_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy component recombination candidate plan",
        payload,
    )


def _dynamic_strategy_component_recombination_candidate_retest_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2396.DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = m2396.DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = m2396.DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config-path")
    ] = m2396.DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry-path")
    ] = m2396.DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_recombination_candidate_plan_2395_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-plan-2395")
    ] = m2396.DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH,
    source_candidate_definitions_2395_path: Annotated[
        Path, typer.Option("--source-candidate-definitions-2395")
    ] = m2396.DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH,
    source_retest_plan_2396_path: Annotated[
        Path, typer.Option("--source-retest-plan-2396")
    ] = m2396.DEFAULT_SOURCE_2395_RETEST_PLAN_PATH,
    source_acceptance_criteria_2395_path: Annotated[
        Path, typer.Option("--source-acceptance-criteria-2395")
    ] = m2396.DEFAULT_SOURCE_2395_ACCEPTANCE_CRITERIA_PATH,
    source_owner_review_decision_2394_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2394")
    ] = m2396.DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH,
    source_component_recombination_decision_2394_path: Annotated[
        Path, typer.Option("--source-component-recombination-decision-2394")
    ] = m2396.DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH,
    source_ablation_retest_result_2393_path: Annotated[
        Path, typer.Option("--source-ablation-retest-result-2393")
    ] = m2396.DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH,
    source_component_attribution_matrix_2393_path: Annotated[
        Path, typer.Option("--source-component-attribution-matrix-2393")
    ] = m2396.DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH,
    source_expanded_candidate_retest_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-retest-2386")
    ] = m2396.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH,
    source_expanded_candidate_ranking_2386_path: Annotated[
        Path, typer.Option("--source-expanded-candidate-ranking-2386")
    ] = m2396.DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = m2396.run_dynamic_strategy_component_recombination_candidate_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        source_recombination_candidate_plan_2395_path=(
            source_recombination_candidate_plan_2395_path
        ),
        source_candidate_definitions_2395_path=source_candidate_definitions_2395_path,
        source_retest_plan_2396_path=source_retest_plan_2396_path,
        source_acceptance_criteria_2395_path=source_acceptance_criteria_2395_path,
        source_owner_review_decision_2394_path=source_owner_review_decision_2394_path,
        source_component_recombination_decision_2394_path=(
            source_component_recombination_decision_2394_path
        ),
        source_ablation_retest_result_2393_path=source_ablation_retest_result_2393_path,
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_expanded_candidate_ranking_2386_path=(
            source_expanded_candidate_ranking_2386_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_date_range_kwargs(as_of, start_date, end_date),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy component recombination candidate retest",
        payload,
    )


def _dynamic_strategy_recombination_candidate_owner_review_decision_command(
    source_recombination_retest_result_2396_path: Annotated[
        Path, typer.Option("--source-recombination-retest-result-2396")
    ] = m2397.DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH,
    source_recombination_candidate_ranking_2396_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-ranking-2396")
    ] = m2397.DEFAULT_SOURCE_2396_RECOMBINATION_CANDIDATE_RANKING_PATH,
    source_component_evidence_matrix_2396_path: Annotated[
        Path, typer.Option("--source-component-evidence-matrix-2396")
    ] = m2397.DEFAULT_SOURCE_2396_COMPONENT_EVIDENCE_MATRIX_PATH,
    source_decision_update_2396_path: Annotated[
        Path, typer.Option("--source-decision-update-2396")
    ] = m2397.DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH,
    source_recombination_candidate_plan_2395_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-plan-2395")
    ] = m2397.DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH,
    source_candidate_definitions_2395_path: Annotated[
        Path, typer.Option("--source-candidate-definitions-2395")
    ] = m2397.DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH,
    source_owner_review_decision_2394_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2394")
    ] = m2397.DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH,
    source_component_recombination_decision_2394_path: Annotated[
        Path, typer.Option("--source-component-recombination-decision-2394")
    ] = m2397.DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH,
    source_ablation_retest_result_2393_path: Annotated[
        Path, typer.Option("--source-ablation-retest-result-2393")
    ] = m2397.DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH,
    source_component_attribution_matrix_2393_path: Annotated[
        Path, typer.Option("--source-component-attribution-matrix-2393")
    ] = m2397.DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2397.run_dynamic_strategy_recombination_candidate_owner_review_decision(
        source_recombination_retest_result_2396_path=(
            source_recombination_retest_result_2396_path
        ),
        source_recombination_candidate_ranking_2396_path=(
            source_recombination_candidate_ranking_2396_path
        ),
        source_component_evidence_matrix_2396_path=(
            source_component_evidence_matrix_2396_path
        ),
        source_decision_update_2396_path=source_decision_update_2396_path,
        source_recombination_candidate_plan_2395_path=(
            source_recombination_candidate_plan_2395_path
        ),
        source_candidate_definitions_2395_path=source_candidate_definitions_2395_path,
        source_owner_review_decision_2394_path=source_owner_review_decision_2394_path,
        source_component_recombination_decision_2394_path=(
            source_component_recombination_decision_2394_path
        ),
        source_ablation_retest_result_2393_path=source_ablation_retest_result_2393_path,
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy recombination candidate owner review decision",
        payload,
    )


def _dynamic_strategy_recombination_candidate_gate_evidence_plan_command(
    source_owner_review_decision_2397_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2397")
    ] = m2398.DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH,
    source_gate_evidence_gap_summary_2397_path: Annotated[
        Path, typer.Option("--source-gate-evidence-gap-summary-2397")
    ] = m2398.DEFAULT_SOURCE_2397_GATE_EVIDENCE_GAP_SUMMARY_PATH,
    source_observation_non_approval_record_2397_path: Annotated[
        Path, typer.Option("--source-observation-non-approval-record-2397")
    ] = m2398.DEFAULT_SOURCE_2397_OBSERVATION_NON_APPROVAL_RECORD_PATH,
    source_next_route_2397_path: Annotated[
        Path, typer.Option("--source-next-route-2397")
    ] = m2398.DEFAULT_SOURCE_2397_NEXT_ROUTE_PATH,
    source_recombination_retest_result_2396_path: Annotated[
        Path, typer.Option("--source-recombination-retest-result-2396")
    ] = m2398.DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH,
    source_recombination_candidate_ranking_2396_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-ranking-2396")
    ] = m2398.DEFAULT_SOURCE_2396_RECOMBINATION_CANDIDATE_RANKING_PATH,
    source_component_evidence_matrix_2396_path: Annotated[
        Path, typer.Option("--source-component-evidence-matrix-2396")
    ] = m2398.DEFAULT_SOURCE_2396_COMPONENT_EVIDENCE_MATRIX_PATH,
    source_decision_update_2396_path: Annotated[
        Path, typer.Option("--source-decision-update-2396")
    ] = m2398.DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH,
    source_recombination_candidate_plan_2395_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-plan-2395")
    ] = m2398.DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH,
    source_candidate_definitions_2395_path: Annotated[
        Path, typer.Option("--source-candidate-definitions-2395")
    ] = m2398.DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH,
    source_ablation_retest_result_2393_path: Annotated[
        Path, typer.Option("--source-ablation-retest-result-2393")
    ] = m2398.DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH,
    source_component_attribution_matrix_2393_path: Annotated[
        Path, typer.Option("--source-component-attribution-matrix-2393")
    ] = m2398.DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2398.run_dynamic_strategy_recombination_candidate_gate_evidence_plan(
        source_owner_review_decision_2397_path=source_owner_review_decision_2397_path,
        source_gate_evidence_gap_summary_2397_path=(
            source_gate_evidence_gap_summary_2397_path
        ),
        source_observation_non_approval_record_2397_path=(
            source_observation_non_approval_record_2397_path
        ),
        source_next_route_2397_path=source_next_route_2397_path,
        source_recombination_retest_result_2396_path=(
            source_recombination_retest_result_2396_path
        ),
        source_recombination_candidate_ranking_2396_path=(
            source_recombination_candidate_ranking_2396_path
        ),
        source_component_evidence_matrix_2396_path=(
            source_component_evidence_matrix_2396_path
        ),
        source_decision_update_2396_path=source_decision_update_2396_path,
        source_recombination_candidate_plan_2395_path=(
            source_recombination_candidate_plan_2395_path
        ),
        source_candidate_definitions_2395_path=source_candidate_definitions_2395_path,
        source_ablation_retest_result_2393_path=source_ablation_retest_result_2393_path,
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy recombination candidate gate evidence plan",
        payload,
    )


def _dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2399.DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = m2399.DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = m2399.DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config-path")
    ] = m2399.DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry-path")
    ] = m2399.DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_gate_evidence_plan_result_2398_path: Annotated[
        Path, typer.Option("--source-gate-evidence-plan-result-2398")
    ] = m2399.DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH,
    source_gate_evidence_gap_summary_2398_path: Annotated[
        Path, typer.Option("--source-gate-evidence-gap-summary-2398")
    ] = m2399.DEFAULT_SOURCE_2398_GATE_EVIDENCE_GAP_SUMMARY_PATH,
    source_targeted_improvement_plan_2398_path: Annotated[
        Path, typer.Option("--source-targeted-improvement-plan-2398")
    ] = m2399.DEFAULT_SOURCE_2398_TARGETED_IMPROVEMENT_PLAN_PATH,
    source_retest_plan_2399_2398_path: Annotated[
        Path, typer.Option("--source-retest-plan-2399-2398")
    ] = m2399.DEFAULT_SOURCE_2398_RETEST_PLAN_2399_PATH,
    source_next_route_2398_path: Annotated[
        Path, typer.Option("--source-next-route-2398")
    ] = m2399.DEFAULT_SOURCE_2398_NEXT_ROUTE_PATH,
    source_owner_review_decision_2397_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2397")
    ] = m2399.DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH,
    source_recombination_retest_result_2396_path: Annotated[
        Path, typer.Option("--source-recombination-retest-result-2396")
    ] = m2399.DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH,
    source_recombination_candidate_ranking_2396_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-ranking-2396")
    ] = m2399.DEFAULT_SOURCE_2396_RECOMBINATION_CANDIDATE_RANKING_PATH,
    source_component_evidence_matrix_2396_path: Annotated[
        Path, typer.Option("--source-component-evidence-matrix-2396")
    ] = m2399.DEFAULT_SOURCE_2396_COMPONENT_EVIDENCE_MATRIX_PATH,
    source_decision_update_2396_path: Annotated[
        Path, typer.Option("--source-decision-update-2396")
    ] = m2399.DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH,
    source_recombination_candidate_plan_2395_path: Annotated[
        Path, typer.Option("--source-recombination-candidate-plan-2395")
    ] = m2399.DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH,
    source_candidate_definitions_2395_path: Annotated[
        Path, typer.Option("--source-candidate-definitions-2395")
    ] = m2399.DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = (
        m2399.run_dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            policy_registry_path=policy_registry_path,
            source_gate_evidence_plan_result_2398_path=(
                source_gate_evidence_plan_result_2398_path
            ),
            source_gate_evidence_gap_summary_2398_path=(
                source_gate_evidence_gap_summary_2398_path
            ),
            source_targeted_improvement_plan_2398_path=(
                source_targeted_improvement_plan_2398_path
            ),
            source_retest_plan_2399_2398_path=source_retest_plan_2399_2398_path,
            source_next_route_2398_path=source_next_route_2398_path,
            source_owner_review_decision_2397_path=(
                source_owner_review_decision_2397_path
            ),
            source_recombination_retest_result_2396_path=(
                source_recombination_retest_result_2396_path
            ),
            source_recombination_candidate_ranking_2396_path=(
                source_recombination_candidate_ranking_2396_path
            ),
            source_component_evidence_matrix_2396_path=(
                source_component_evidence_matrix_2396_path
            ),
            source_decision_update_2396_path=source_decision_update_2396_path,
            source_recombination_candidate_plan_2395_path=(
                source_recombination_candidate_plan_2395_path
            ),
            source_candidate_definitions_2395_path=source_candidate_definitions_2395_path,
            output_root=output_root,
            docs_root=docs_root,
            **_date_range_kwargs(as_of, start_date, end_date),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy recombination candidate targeted gate evidence retest",
        payload,
    )


def _dynamic_strategy_targeted_gate_evidence_owner_review_decision_command(
    source_targeted_retest_result_2399_path: Annotated[
        Path, typer.Option("--source-targeted-retest-result-2399")
    ] = m2400.DEFAULT_SOURCE_2399_TARGETED_RETEST_RESULT_PATH,
    source_targeted_variant_ranking_2399_path: Annotated[
        Path, typer.Option("--source-targeted-variant-ranking-2399")
    ] = m2400.DEFAULT_SOURCE_2399_TARGETED_VARIANT_RANKING_PATH,
    source_gate_evidence_matrix_2399_path: Annotated[
        Path, typer.Option("--source-gate-evidence-matrix-2399")
    ] = m2400.DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH,
    source_decision_update_2399_path: Annotated[
        Path, typer.Option("--source-decision-update-2399")
    ] = m2400.DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_gate_evidence_plan_result_2398_path: Annotated[
        Path, typer.Option("--source-gate-evidence-plan-result-2398")
    ] = m2400.DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH,
    source_targeted_improvement_plan_2398_path: Annotated[
        Path, typer.Option("--source-targeted-improvement-plan-2398")
    ] = m2400.DEFAULT_SOURCE_2398_TARGETED_IMPROVEMENT_PLAN_PATH,
    source_next_route_2398_path: Annotated[
        Path, typer.Option("--source-next-route-2398")
    ] = m2400.DEFAULT_SOURCE_2398_NEXT_ROUTE_PATH,
    source_owner_review_decision_2397_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2397")
    ] = m2400.DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH,
    source_recombination_retest_result_2396_path: Annotated[
        Path, typer.Option("--source-recombination-retest-result-2396")
    ] = m2400.DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH,
    source_decision_update_2396_path: Annotated[
        Path, typer.Option("--source-decision-update-2396")
    ] = m2400.DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2400.DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2400.run_dynamic_strategy_targeted_gate_evidence_owner_review_decision(
            source_targeted_retest_result_2399_path=(
                source_targeted_retest_result_2399_path
            ),
            source_targeted_variant_ranking_2399_path=(
                source_targeted_variant_ranking_2399_path
            ),
            source_gate_evidence_matrix_2399_path=(
                source_gate_evidence_matrix_2399_path
            ),
            source_decision_update_2399_path=source_decision_update_2399_path,
            source_gate_evidence_plan_result_2398_path=(
                source_gate_evidence_plan_result_2398_path
            ),
            source_targeted_improvement_plan_2398_path=(
                source_targeted_improvement_plan_2398_path
            ),
            source_next_route_2398_path=source_next_route_2398_path,
            source_owner_review_decision_2397_path=(
                source_owner_review_decision_2397_path
            ),
            source_recombination_retest_result_2396_path=(
                source_recombination_retest_result_2396_path
            ),
            source_decision_update_2396_path=source_decision_update_2396_path,
            output_root=output_root,
            docs_root=docs_root,
            **_as_of_kwargs(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy targeted gate evidence owner review decision",
        payload,
    )


def _dynamic_strategy_recombination_line_plateau_decision_command(
    source_owner_review_decision_2400_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2400")
    ] = m2401.DEFAULT_SOURCE_2400_OWNER_REVIEW_DECISION_PATH,
    source_observation_non_approval_record_2400_path: Annotated[
        Path, typer.Option("--source-observation-non-approval-record-2400")
    ] = m2401.DEFAULT_SOURCE_2400_OBSERVATION_NON_APPROVAL_RECORD_PATH,
    source_targeted_improvement_value_summary_2400_path: Annotated[
        Path, typer.Option("--source-targeted-improvement-value-summary-2400")
    ] = m2401.DEFAULT_SOURCE_2400_TARGETED_IMPROVEMENT_VALUE_SUMMARY_PATH,
    source_next_route_2400_path: Annotated[
        Path, typer.Option("--source-next-route-2400")
    ] = m2401.DEFAULT_SOURCE_2400_NEXT_ROUTE_PATH,
    source_targeted_retest_result_2399_path: Annotated[
        Path, typer.Option("--source-targeted-retest-result-2399")
    ] = m2401.DEFAULT_SOURCE_2399_TARGETED_RETEST_RESULT_PATH,
    source_targeted_variant_ranking_2399_path: Annotated[
        Path, typer.Option("--source-targeted-variant-ranking-2399")
    ] = m2401.DEFAULT_SOURCE_2399_TARGETED_VARIANT_RANKING_PATH,
    source_decision_update_2399_path: Annotated[
        Path, typer.Option("--source-decision-update-2399")
    ] = m2401.DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_gate_evidence_plan_result_2398_path: Annotated[
        Path, typer.Option("--source-gate-evidence-plan-result-2398")
    ] = m2401.DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH,
    source_owner_review_decision_2397_path: Annotated[
        Path, typer.Option("--source-owner-review-decision-2397")
    ] = m2401.DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH,
    source_recombination_retest_result_2396_path: Annotated[
        Path, typer.Option("--source-recombination-retest-result-2396")
    ] = m2401.DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2401.run_dynamic_strategy_recombination_line_plateau_decision(
        source_owner_review_decision_2400_path=source_owner_review_decision_2400_path,
        source_observation_non_approval_record_2400_path=(
            source_observation_non_approval_record_2400_path
        ),
        source_targeted_improvement_value_summary_2400_path=(
            source_targeted_improvement_value_summary_2400_path
        ),
        source_next_route_2400_path=source_next_route_2400_path,
        source_targeted_retest_result_2399_path=source_targeted_retest_result_2399_path,
        source_targeted_variant_ranking_2399_path=(
            source_targeted_variant_ranking_2399_path
        ),
        source_decision_update_2399_path=source_decision_update_2399_path,
        source_gate_evidence_plan_result_2398_path=(
            source_gate_evidence_plan_result_2398_path
        ),
        source_owner_review_decision_2397_path=source_owner_review_decision_2397_path,
        source_recombination_retest_result_2396_path=(
            source_recombination_retest_result_2396_path
        ),
        output_root=output_root,
        docs_root=docs_root,
        **_as_of_kwargs(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy recombination line plateau 决策",
        payload,
    )


def _dynamic_strategy_data_pit_signal_quality_gap_review_command(
    source_plateau_decision_2401_path: Annotated[
        Path, typer.Option("--source-plateau-decision-2401")
    ] = m2402.DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH,
    source_route_2401_path: Annotated[
        Path, typer.Option("--source-route-2401")
    ] = m2402.DEFAULT_SOURCE_2401_ROUTE_PATH,
    source_owner_review_2400_path: Annotated[
        Path, typer.Option("--source-owner-review-2400")
    ] = m2402.DEFAULT_SOURCE_2400_OWNER_REVIEW_PATH,
    source_targeted_retest_2399_path: Annotated[
        Path, typer.Option("--source-targeted-retest-2399")
    ] = m2402.DEFAULT_SOURCE_2399_TARGETED_RETEST_PATH,
    source_gate_evidence_matrix_2399_path: Annotated[
        Path, typer.Option("--source-gate-evidence-matrix-2399")
    ] = m2402.DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH,
    source_decision_update_2399_path: Annotated[
        Path, typer.Option("--source-decision-update-2399")
    ] = m2402.DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_expanded_retest_2386_path: Annotated[
        Path, typer.Option("--source-expanded-retest-2386")
    ] = m2402.DEFAULT_SOURCE_2386_EXPANDED_RETEST_PATH,
    source_signal_family_screening_2386_path: Annotated[
        Path, typer.Option("--source-signal-family-screening-2386")
    ] = m2402.DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH,
    source_cadence_bias_audit_2364_path: Annotated[
        Path, typer.Option("--source-cadence-bias-audit-2364")
    ] = m2402.DEFAULT_SOURCE_2364_CADENCE_BIAS_AUDIT_PATH,
    source_validate_data_audit_path: Annotated[
        Path | None, typer.Option("--source-validate-data-audit")
    ] = None,
    source_validate_data_report_path: Annotated[
        Path | None, typer.Option("--source-validate-data-report")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    validate_data_as_of: Annotated[str, typer.Option("--validate-data-as-of")] = (
        m2402.DEFAULT_DATA_QUALITY_AS_OF.isoformat()
    ),
) -> None:
    payload = m2402.run_dynamic_strategy_data_pit_signal_quality_gap_review(
        source_plateau_decision_2401_path=source_plateau_decision_2401_path,
        source_route_2401_path=source_route_2401_path,
        source_owner_review_2400_path=source_owner_review_2400_path,
        source_targeted_retest_2399_path=source_targeted_retest_2399_path,
        source_gate_evidence_matrix_2399_path=source_gate_evidence_matrix_2399_path,
        source_decision_update_2399_path=source_decision_update_2399_path,
        source_expanded_retest_2386_path=source_expanded_retest_2386_path,
        source_signal_family_screening_2386_path=(
            source_signal_family_screening_2386_path
        ),
        source_cadence_bias_audit_2364_path=source_cadence_bias_audit_2364_path,
        source_validate_data_audit_path=source_validate_data_audit_path,
        source_validate_data_report_path=source_validate_data_report_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
        validate_data_as_of=(
            _parse_optional_date(validate_data_as_of)
            or m2402.DEFAULT_DATA_QUALITY_AS_OF
        ),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy data / PIT / signal quality gap review",
        payload,
    )


def _dynamic_strategy_pit_coverage_signal_construction_review_command(
    source_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-gap-review-2402")
    ] = m2403.DEFAULT_SOURCE_2402_GAP_REVIEW_PATH,
    source_pit_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-pit-gap-review-2402")
    ] = m2403.DEFAULT_SOURCE_2402_PIT_GAP_REVIEW_PATH,
    source_signal_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-signal-gap-review-2402")
    ] = m2403.DEFAULT_SOURCE_2402_SIGNAL_GAP_REVIEW_PATH,
    source_regime_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-regime-gap-review-2402")
    ] = m2403.DEFAULT_SOURCE_2402_REGIME_GAP_REVIEW_PATH,
    source_threshold_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-threshold-gap-review-2402")
    ] = m2403.DEFAULT_SOURCE_2402_THRESHOLD_GAP_REVIEW_PATH,
    source_plateau_decision_2401_path: Annotated[
        Path, typer.Option("--source-plateau-decision-2401")
    ] = m2403.DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH,
    source_targeted_retest_2399_path: Annotated[
        Path, typer.Option("--source-targeted-retest-2399")
    ] = m2403.DEFAULT_SOURCE_2399_TARGETED_RETEST_PATH,
    source_gate_evidence_matrix_2399_path: Annotated[
        Path, typer.Option("--source-gate-evidence-matrix-2399")
    ] = m2403.DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH,
    source_decision_update_2399_path: Annotated[
        Path, typer.Option("--source-decision-update-2399")
    ] = m2403.DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_expanded_retest_2386_path: Annotated[
        Path, typer.Option("--source-expanded-retest-2386")
    ] = m2403.DEFAULT_SOURCE_2386_EXPANDED_RETEST_PATH,
    source_signal_family_screening_2386_path: Annotated[
        Path, typer.Option("--source-signal-family-screening-2386")
    ] = m2403.DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH,
    source_cadence_bias_audit_2364_path: Annotated[
        Path, typer.Option("--source-cadence-bias-audit-2364")
    ] = m2403.DEFAULT_SOURCE_2364_CADENCE_BIAS_AUDIT_PATH,
    source_validate_data_audit_path: Annotated[
        Path | None, typer.Option("--source-validate-data-audit")
    ] = None,
    source_validate_data_report_path: Annotated[
        Path | None, typer.Option("--source-validate-data-report")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    validate_data_as_of: Annotated[str, typer.Option("--validate-data-as-of")] = (
        m2403.DEFAULT_DATA_QUALITY_AS_OF.isoformat()
    ),
) -> None:
    payload = m2403.run_dynamic_strategy_pit_coverage_signal_construction_review(
        source_gap_review_2402_path=source_gap_review_2402_path,
        source_pit_gap_review_2402_path=source_pit_gap_review_2402_path,
        source_signal_gap_review_2402_path=source_signal_gap_review_2402_path,
        source_regime_gap_review_2402_path=source_regime_gap_review_2402_path,
        source_threshold_gap_review_2402_path=source_threshold_gap_review_2402_path,
        source_plateau_decision_2401_path=source_plateau_decision_2401_path,
        source_targeted_retest_2399_path=source_targeted_retest_2399_path,
        source_gate_evidence_matrix_2399_path=source_gate_evidence_matrix_2399_path,
        source_decision_update_2399_path=source_decision_update_2399_path,
        source_expanded_retest_2386_path=source_expanded_retest_2386_path,
        source_signal_family_screening_2386_path=(
            source_signal_family_screening_2386_path
        ),
        source_cadence_bias_audit_2364_path=source_cadence_bias_audit_2364_path,
        source_validate_data_audit_path=source_validate_data_audit_path,
        source_validate_data_report_path=source_validate_data_report_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
        validate_data_as_of=(
            _parse_optional_date(validate_data_as_of)
            or m2403.DEFAULT_DATA_QUALITY_AS_OF
        ),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy PIT coverage / signal construction review",
        payload,
    )


def _dynamic_strategy_pit_coverage_matrix_implementation_plan_command(
    source_review_2403_path: Annotated[
        Path, typer.Option("--source-review-2403")
    ] = m2404.DEFAULT_SOURCE_2403_REVIEW_PATH,
    source_pit_matrix_2403_path: Annotated[
        Path, typer.Option("--source-pit-matrix-2403")
    ] = m2404.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_signal_review_2403_path: Annotated[
        Path, typer.Option("--source-signal-review-2403")
    ] = m2404.DEFAULT_SOURCE_2403_SIGNAL_REVIEW_PATH,
    source_regime_review_2403_path: Annotated[
        Path, typer.Option("--source-regime-review-2403")
    ] = m2404.DEFAULT_SOURCE_2403_REGIME_REVIEW_PATH,
    source_remediation_matrix_2403_path: Annotated[
        Path, typer.Option("--source-remediation-matrix-2403")
    ] = m2404.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    source_threshold_gap_2403_path: Annotated[
        Path, typer.Option("--source-threshold-gap-2403")
    ] = m2404.DEFAULT_SOURCE_2403_THRESHOLD_GAP_PATH,
    source_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-gap-review-2402")
    ] = m2404.DEFAULT_SOURCE_2402_GAP_REVIEW_PATH,
    source_data_quality_gap_matrix_2402_path: Annotated[
        Path, typer.Option("--source-data-quality-gap-matrix-2402")
    ] = m2404.DEFAULT_SOURCE_2402_DATA_QUALITY_GAP_MATRIX_PATH,
    source_pit_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-pit-gap-review-2402")
    ] = m2404.DEFAULT_SOURCE_2402_PIT_GAP_REVIEW_PATH,
    source_signal_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-signal-gap-review-2402")
    ] = m2404.DEFAULT_SOURCE_2402_SIGNAL_GAP_REVIEW_PATH,
    source_regime_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-regime-gap-review-2402")
    ] = m2404.DEFAULT_SOURCE_2402_REGIME_GAP_REVIEW_PATH,
    source_threshold_gap_review_2402_path: Annotated[
        Path, typer.Option("--source-threshold-gap-review-2402")
    ] = m2404.DEFAULT_SOURCE_2402_THRESHOLD_GAP_REVIEW_PATH,
    source_plateau_decision_2401_path: Annotated[
        Path, typer.Option("--source-plateau-decision-2401")
    ] = m2404.DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH,
    source_next_direction_2401_path: Annotated[
        Path, typer.Option("--source-next-direction-2401")
    ] = m2404.DEFAULT_SOURCE_2401_NEXT_DIRECTION_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2404.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2404.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2404.run_dynamic_strategy_pit_coverage_matrix_implementation_plan(
        source_review_2403_path=source_review_2403_path,
        source_pit_matrix_2403_path=source_pit_matrix_2403_path,
        source_signal_review_2403_path=source_signal_review_2403_path,
        source_regime_review_2403_path=source_regime_review_2403_path,
        source_remediation_matrix_2403_path=source_remediation_matrix_2403_path,
        source_threshold_gap_2403_path=source_threshold_gap_2403_path,
        source_gap_review_2402_path=source_gap_review_2402_path,
        source_data_quality_gap_matrix_2402_path=(
            source_data_quality_gap_matrix_2402_path
        ),
        source_pit_gap_review_2402_path=source_pit_gap_review_2402_path,
        source_signal_gap_review_2402_path=source_signal_gap_review_2402_path,
        source_regime_gap_review_2402_path=source_regime_gap_review_2402_path,
        source_threshold_gap_review_2402_path=source_threshold_gap_review_2402_path,
        source_plateau_decision_2401_path=source_plateau_decision_2401_path,
        source_next_direction_2401_path=source_next_direction_2401_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy PIT coverage matrix implementation plan",
        payload,
    )


def _dynamic_strategy_pit_coverage_matrix_generate_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    source_2404_implementation_path: Annotated[
        Path, typer.Option("--source-2404-implementation")
    ] = m2405.DEFAULT_SOURCE_2404_IMPLEMENTATION_PATH,
    source_2404_registry_schema_path: Annotated[
        Path, typer.Option("--source-2404-registry-schema")
    ] = m2405.DEFAULT_SOURCE_2404_REGISTRY_SCHEMA_PATH,
    source_2404_gate_policy_path: Annotated[
        Path, typer.Option("--source-2404-gate-policy")
    ] = m2405.DEFAULT_SOURCE_2404_GATE_POLICY_PATH,
    source_2404_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2404-blocker-summary")
    ] = m2405.DEFAULT_SOURCE_2404_BLOCKER_SUMMARY_PATH,
    source_2403_pit_matrix_path: Annotated[
        Path, typer.Option("--source-2403-pit-matrix")
    ] = m2405.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_remediation_matrix_path: Annotated[
        Path, typer.Option("--source-2403-remediation-matrix")
    ] = m2405.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    ),
    research_quality_output_root: Annotated[
        Path, typer.Option("--research-quality-output-root")
    ] = m2405.DEFAULT_RESEARCH_QUALITY_PIT_COVERAGE_MATRIX_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2405.run_dynamic_strategy_pit_coverage_matrix_reusable_implementation(
        registry_path=registry_path,
        source_2404_implementation_path=source_2404_implementation_path,
        source_2404_registry_schema_path=source_2404_registry_schema_path,
        source_2404_gate_policy_path=source_2404_gate_policy_path,
        source_2404_blocker_summary_path=source_2404_blocker_summary_path,
        source_2403_pit_matrix_path=source_2403_pit_matrix_path,
        source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
        output_root=output_root,
        research_quality_output_root=research_quality_output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy PIT coverage matrix reusable implementation",
        payload,
    )


def _dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan_command(
    source_2405_implementation_path: Annotated[
        Path, typer.Option("--source-2405-implementation")
    ] = m2406.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2406.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Annotated[
        Path, typer.Option("--source-2405-pit-matrix")
    ] = m2406.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2406.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2406.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Annotated[
        Path, typer.Option("--source-2405-remediation-routes")
    ] = m2406.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2403_pit_matrix_path: Annotated[
        Path, typer.Option("--source-2403-pit-matrix")
    ] = m2406.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_signal_construction_review_path: Annotated[
        Path, typer.Option("--source-2403-signal-construction-review")
    ] = m2406.DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH,
    source_2403_remediation_matrix_path: Annotated[
        Path, typer.Option("--source-2403-remediation-matrix")
    ] = m2406.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2406.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    growth_tilt_config_path: Annotated[
        Path, typer.Option("--growth-tilt-config")
    ] = m2406.DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    execution_policy_registry_path: Annotated[
        Path, typer.Option("--execution-policy-registry")
    ] = m2406.DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2406.run_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan(
            source_2405_implementation_path=source_2405_implementation_path,
            source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
            source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
            source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
            source_2405_blocker_summary_path=source_2405_blocker_summary_path,
            source_2405_remediation_routes_path=source_2405_remediation_routes_path,
            source_2403_pit_matrix_path=source_2403_pit_matrix_path,
            source_2403_signal_construction_review_path=(
                source_2403_signal_construction_review_path
            ),
            source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
            pit_input_registry_path=pit_input_registry_path,
            growth_tilt_config_path=growth_tilt_config_path,
            execution_policy_registry_path=execution_policy_registry_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy growth tilt engine PIT remediation plan",
        payload,
    )


def _dynamic_strategy_valid_until_window_stale_signal_remediation_plan_command(
    source_2405_implementation_path: Annotated[
        Path, typer.Option("--source-2405-implementation")
    ] = m2407.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2407.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Annotated[
        Path, typer.Option("--source-2405-pit-matrix")
    ] = m2407.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2407.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2407.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Annotated[
        Path, typer.Option("--source-2405-remediation-routes")
    ] = m2407.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2406_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2406-remediation-plan")
    ] = m2407.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2406_source_feature_inventory_path: Annotated[
        Path, typer.Option("--source-2406-source-feature-inventory")
    ] = m2407.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH,
    source_2406_pit_risk_audit_path: Annotated[
        Path, typer.Option("--source-2406-pit-risk-audit")
    ] = m2407.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Annotated[
        Path, typer.Option("--source-2406-signal-construction-gap-analysis")
    ] = m2407.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH,
    source_2406_severity_downgrade_conditions_path: Annotated[
        Path, typer.Option("--source-2406-severity-downgrade-conditions")
    ] = m2407.DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH,
    source_2406_validation_plan_path: Annotated[
        Path, typer.Option("--source-2406-validation-plan")
    ] = m2407.DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH,
    source_2403_pit_matrix_path: Annotated[
        Path, typer.Option("--source-2403-pit-matrix")
    ] = m2407.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_signal_construction_review_path: Annotated[
        Path, typer.Option("--source-2403-signal-construction-review")
    ] = m2407.DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH,
    source_2403_remediation_matrix_path: Annotated[
        Path, typer.Option("--source-2403-remediation-matrix")
    ] = m2407.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2407.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    execution_policy_registry_path: Annotated[
        Path, typer.Option("--execution-policy-registry")
    ] = m2407.DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Annotated[
        Path, typer.Option("--signal-validity-taxonomy")
    ] = m2407.DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2407.run_dynamic_strategy_valid_until_window_stale_signal_remediation_plan(
            source_2405_implementation_path=source_2405_implementation_path,
            source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
            source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
            source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
            source_2405_blocker_summary_path=source_2405_blocker_summary_path,
            source_2405_remediation_routes_path=source_2405_remediation_routes_path,
            source_2406_remediation_plan_path=source_2406_remediation_plan_path,
            source_2406_source_feature_inventory_path=(
                source_2406_source_feature_inventory_path
            ),
            source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
            source_2406_signal_construction_gap_analysis_path=(
                source_2406_signal_construction_gap_analysis_path
            ),
            source_2406_severity_downgrade_conditions_path=(
                source_2406_severity_downgrade_conditions_path
            ),
            source_2406_validation_plan_path=source_2406_validation_plan_path,
            source_2403_pit_matrix_path=source_2403_pit_matrix_path,
            source_2403_signal_construction_review_path=(
                source_2403_signal_construction_review_path
            ),
            source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
            pit_input_registry_path=pit_input_registry_path,
            execution_policy_registry_path=execution_policy_registry_path,
            signal_validity_taxonomy_path=signal_validity_taxonomy_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy valid-until window stale signal remediation plan",
        payload,
    )


def _dynamic_strategy_blocking_gap_remediation_implementation_plan_command(
    source_2405_implementation_path: Annotated[
        Path, typer.Option("--source-2405-implementation")
    ] = m2408.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2408.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Annotated[
        Path, typer.Option("--source-2405-pit-matrix")
    ] = m2408.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2408.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2408.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Annotated[
        Path, typer.Option("--source-2405-remediation-routes")
    ] = m2408.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2406_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2406-remediation-plan")
    ] = m2408.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2406_source_feature_inventory_path: Annotated[
        Path, typer.Option("--source-2406-source-feature-inventory")
    ] = m2408.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH,
    source_2406_pit_risk_audit_path: Annotated[
        Path, typer.Option("--source-2406-pit-risk-audit")
    ] = m2408.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Annotated[
        Path, typer.Option("--source-2406-signal-construction-gap-analysis")
    ] = m2408.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH,
    source_2406_severity_downgrade_conditions_path: Annotated[
        Path, typer.Option("--source-2406-severity-downgrade-conditions")
    ] = m2408.DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH,
    source_2406_validation_plan_path: Annotated[
        Path, typer.Option("--source-2406-validation-plan")
    ] = m2408.DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH,
    source_2407_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2407-remediation-plan")
    ] = m2408.DEFAULT_SOURCE_2407_REMEDIATION_PLAN_PATH,
    source_2407_valid_until_semantics_review_path: Annotated[
        Path, typer.Option("--source-2407-valid-until-semantics-review")
    ] = m2408.DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH,
    source_2407_stale_signal_risk_audit_path: Annotated[
        Path, typer.Option("--source-2407-stale-signal-risk-audit")
    ] = m2408.DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH,
    source_2407_signal_validity_contract_plan_path: Annotated[
        Path, typer.Option("--source-2407-signal-validity-contract-plan")
    ] = m2408.DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH,
    source_2407_severity_downgrade_conditions_path: Annotated[
        Path, typer.Option("--source-2407-severity-downgrade-conditions")
    ] = m2408.DEFAULT_SOURCE_2407_SEVERITY_DOWNGRADE_CONDITIONS_PATH,
    source_2407_validation_plan_path: Annotated[
        Path, typer.Option("--source-2407-validation-plan")
    ] = m2408.DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2408.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2408.run_dynamic_strategy_blocking_gap_remediation_implementation_plan(
        source_2405_implementation_path=source_2405_implementation_path,
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        source_2405_remediation_routes_path=source_2405_remediation_routes_path,
        source_2406_remediation_plan_path=source_2406_remediation_plan_path,
        source_2406_source_feature_inventory_path=(
            source_2406_source_feature_inventory_path
        ),
        source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
        source_2406_signal_construction_gap_analysis_path=(
            source_2406_signal_construction_gap_analysis_path
        ),
        source_2406_severity_downgrade_conditions_path=(
            source_2406_severity_downgrade_conditions_path
        ),
        source_2406_validation_plan_path=source_2406_validation_plan_path,
        source_2407_remediation_plan_path=source_2407_remediation_plan_path,
        source_2407_valid_until_semantics_review_path=(
            source_2407_valid_until_semantics_review_path
        ),
        source_2407_stale_signal_risk_audit_path=(
            source_2407_stale_signal_risk_audit_path
        ),
        source_2407_signal_validity_contract_plan_path=(
            source_2407_signal_validity_contract_plan_path
        ),
        source_2407_severity_downgrade_conditions_path=(
            source_2407_severity_downgrade_conditions_path
        ),
        source_2407_validation_plan_path=source_2407_validation_plan_path,
        pit_input_registry_path=pit_input_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy blocking gap remediation implementation plan",
        payload,
    )


def _dynamic_strategy_signal_as_of_validity_contract_schema_command(
    source_2408_implementation_plan_path: Annotated[
        Path, typer.Option("--source-2408-implementation-plan")
    ] = m2409.DEFAULT_SOURCE_2408_IMPLEMENTATION_PLAN_PATH,
    source_2408_contract_schema_plan_path: Annotated[
        Path, typer.Option("--source-2408-contract-schema-plan")
    ] = m2409.DEFAULT_SOURCE_2408_CONTRACT_SCHEMA_PLAN_PATH,
    source_2408_candidate_search_gate_policy_path: Annotated[
        Path, typer.Option("--source-2408-candidate-search-gate-policy")
    ] = m2409.DEFAULT_SOURCE_2408_CANDIDATE_SEARCH_GATE_POLICY_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2409.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2409.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2409.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2409.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_DOCS_ROOT,
    research_quality_output_root: Annotated[
        Path, typer.Option("--research-quality-output-root")
    ] = m2409.DEFAULT_SIGNAL_CONTRACT_RESEARCH_QUALITY_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2409.run_dynamic_strategy_signal_as_of_validity_contract_schema(
        source_2408_implementation_plan_path=source_2408_implementation_plan_path,
        source_2408_contract_schema_plan_path=source_2408_contract_schema_plan_path,
        source_2408_candidate_search_gate_policy_path=(
            source_2408_candidate_search_gate_policy_path
        ),
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        pit_input_registry_path=pit_input_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        research_quality_output_root=research_quality_output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy signal as-of and validity contract schema",
        payload,
    )


def _growth_tilt_engine_source_feature_contract_mapping_command(
    source_2409_contract_schema_result_path: Annotated[
        Path, typer.Option("--source-2409-contract-schema-result")
    ] = m2410.DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_RESULT_PATH,
    source_2409_source_feature_contract_schema_path: Annotated[
        Path, typer.Option("--source-2409-source-feature-contract-schema")
    ] = m2410.DEFAULT_SOURCE_2409_SOURCE_FEATURE_CONTRACT_SCHEMA_PATH,
    source_2409_signal_as_of_contract_schema_path: Annotated[
        Path, typer.Option("--source-2409-signal-as-of-contract-schema")
    ] = m2410.DEFAULT_SOURCE_2409_SIGNAL_AS_OF_CONTRACT_SCHEMA_PATH,
    source_2409_signal_validity_contract_schema_path: Annotated[
        Path, typer.Option("--source-2409-signal-validity-contract-schema")
    ] = m2410.DEFAULT_SOURCE_2409_SIGNAL_VALIDITY_CONTRACT_SCHEMA_PATH,
    source_2409_contract_schema_snapshot_path: Annotated[
        Path, typer.Option("--source-2409-contract-schema-snapshot")
    ] = m2410.DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_SNAPSHOT_PATH,
    source_2406_source_feature_inventory_path: Annotated[
        Path, typer.Option("--source-2406-source-feature-inventory")
    ] = m2410.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH,
    source_2406_pit_risk_audit_path: Annotated[
        Path, typer.Option("--source-2406-pit-risk-audit")
    ] = m2410.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Annotated[
        Path, typer.Option("--source-2406-signal-construction-gap-analysis")
    ] = m2410.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH,
    source_2406_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2406-remediation-plan")
    ] = m2410.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2410.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2410.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2410.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    growth_tilt_candidate_registry_path: Annotated[
        Path, typer.Option("--growth-tilt-candidate-registry")
    ] = m2410.DEFAULT_EQUAL_RISK_GROWTH_TILT_CANDIDATE_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2410.run_growth_tilt_engine_source_feature_contract_mapping(
        source_2409_contract_schema_result_path=source_2409_contract_schema_result_path,
        source_2409_source_feature_contract_schema_path=(
            source_2409_source_feature_contract_schema_path
        ),
        source_2409_signal_as_of_contract_schema_path=(
            source_2409_signal_as_of_contract_schema_path
        ),
        source_2409_signal_validity_contract_schema_path=(
            source_2409_signal_validity_contract_schema_path
        ),
        source_2409_contract_schema_snapshot_path=(
            source_2409_contract_schema_snapshot_path
        ),
        source_2406_source_feature_inventory_path=(
            source_2406_source_feature_inventory_path
        ),
        source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
        source_2406_signal_construction_gap_analysis_path=(
            source_2406_signal_construction_gap_analysis_path
        ),
        source_2406_remediation_plan_path=source_2406_remediation_plan_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        pit_input_registry_path=pit_input_registry_path,
        growth_tilt_candidate_registry_path=growth_tilt_candidate_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine source feature contract mapping",
        payload,
    )
    for field in (
        "blockers_resolved",
        "blockers_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_contract_gap_remediation_plan_command(
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2411.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Annotated[
        Path, typer.Option("--source-2410-source-feature-contract-mapping")
    ] = m2411.DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH,
    source_2410_contract_mapping_validation_path: Annotated[
        Path, typer.Option("--source-2410-contract-mapping-validation")
    ] = m2411.DEFAULT_SOURCE_2410_CONTRACT_MAPPING_VALIDATION_PATH,
    source_2410_unresolved_gap_summary_path: Annotated[
        Path, typer.Option("--source-2410-unresolved-gap-summary")
    ] = m2411.DEFAULT_SOURCE_2410_UNRESOLVED_GAP_SUMMARY_PATH,
    source_2410_research_doc_path: Annotated[
        Path, typer.Option("--source-2410-research-doc")
    ] = m2411.DEFAULT_SOURCE_2410_RESEARCH_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2411.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2411.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2411.run_growth_tilt_engine_contract_gap_remediation_plan(
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        source_2410_contract_mapping_validation_path=(
            source_2410_contract_mapping_validation_path
        ),
        source_2410_unresolved_gap_summary_path=(
            source_2410_unresolved_gap_summary_path
        ),
        source_2410_research_doc_path=source_2410_research_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine contract gap remediation plan",
        payload,
    )
    for field in (
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _call_builder(
    builder: Callable[..., dict[str, object]],
    kwargs: dict[str, object],
) -> dict[str, object]:
    accepted = set(inspect.signature(builder).parameters)
    return builder(**{key: value for key, value in kwargs.items() if key in accepted})


def _date_range_kwargs(
    as_of: str | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, date | None]:
    return {
        "as_of_date": _parse_optional_date(as_of),
        "start_date": _parse_optional_date(start_date)
        or DEFAULT_AI_REGIME_BACKTEST_START,
        "end_date": _parse_optional_date(end_date),
    }


def _as_of_kwargs(as_of: str | None) -> dict[str, date | None]:
    return {"as_of_date": _parse_optional_date(as_of)}


def _print_execution_semantics_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "PASS" in status or "SAFE" in status else "yellow"
    if "BLOCKED" in status or "FAIL" in status:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        if paths.get("json_path"):
            console.print(f"JSON：{paths.get('json_path')}")
        if paths.get("index"):
            console.print(f"Index：{paths.get('index')}")
        if paths.get("markdown_path"):
            console.print(f"Markdown：{paths.get('markdown_path')}")
        if paths.get("review_markdown"):
            console.print(f"Markdown：{paths.get('review_markdown')}")
        if paths.get("yaml_path"):
            console.print(f"YAML：{paths.get('yaml_path')}")
        if paths.get("review_yaml"):
            console.print(f"YAML：{paths.get('review_yaml')}")
    for field, expected in (
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("manual_review_required", True),
        ("production_effect", "none"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


_EXECUTION_SEMANTICS_COMMANDS: tuple[
    tuple[str, Callable[..., dict[str, object]], str],
    ...,
] = (
    (
        "dynamic-strategy-execution-semantics-contract",
        run_dynamic_strategy_execution_semantics_contract,
        "Dynamic strategy execution semantics contract",
    ),
    (
        "implicit-monthly-rebalance-assumption-audit",
        run_implicit_monthly_rebalance_assumption_audit,
        "Implicit monthly rebalance assumption audit",
    ),
    (
        "strategy-execution-policy-registry-review",
        run_strategy_execution_policy_registry_review,
        "Strategy execution policy registry review",
    ),
    (
        "dynamic-strategy-validity-period-audit",
        run_dynamic_strategy_validity_period_audit,
        "Dynamic strategy validity period audit",
    ),
    (
        "target-vs-actual-position-path-builder",
        run_target_vs_actual_position_path_builder,
        "Target vs actual position path builder",
    ),
    (
        "execution-semantics-rebacktest-gate",
        run_execution_semantics_rebacktest_gate,
        "Execution semantics rebacktest gate",
    ),
    (
        "rebalance-frequency-sensitivity-suite",
        run_rebalance_frequency_sensitivity_suite,
        "Rebalance frequency sensitivity suite",
    ),
    (
        "threshold-hybrid-rebalance-review",
        run_threshold_hybrid_rebalance_review,
        "Threshold hybrid rebalance review",
    ),
    (
        "signal-staleness-cost-review",
        run_signal_staleness_cost_review,
        "Signal staleness cost review",
    ),
    (
        "dynamic-strategy-latency-execution-lag-review",
        run_dynamic_strategy_latency_execution_lag_review,
        "Dynamic strategy latency execution lag review",
    ),
    (
        "execution-policy-impact-on-prior-conclusions",
        run_execution_policy_impact_on_prior_conclusions,
        "Execution policy impact on prior conclusions",
    ),
    (
        "rebalance-sensitive-candidate-recovery-review",
        run_rebalance_sensitive_candidate_recovery_review,
        "Rebalance sensitive candidate recovery review",
    ),
    (
        "execution-semantics-data-lineage-audit",
        run_execution_semantics_data_lineage_audit,
        "Execution semantics data lineage audit",
    ),
    (
        "execution-policy-cost-turnover-normalization",
        run_execution_policy_cost_turnover_normalization,
        "Execution policy cost turnover normalization",
    ),
    (
        "execution-semantics-external-validation-update",
        run_execution_semantics_external_validation_update,
        "Execution semantics external validation update",
    ),
    (
        "execution-aware-forward-aging-observation-contract",
        run_execution_aware_forward_aging_observation_contract,
        "Execution aware forward aging observation contract",
    ),
    (
        "equal-risk-balanced-core-execution-policy-selection",
        run_equal_risk_balanced_core_execution_policy_selection,
        "Equal risk balanced core execution policy selection",
    ),
    (
        "dynamic-backtest-engine-contract-update",
        run_dynamic_backtest_engine_contract_update,
        "Dynamic backtest engine contract update",
    ),
    (
        "execution-semantics-reporting-update",
        run_execution_semantics_reporting_update,
        "Execution semantics reporting update",
    ),
    (
        "rebalance-assumption-owner-review-pack",
        run_rebalance_assumption_owner_review_pack,
        "Rebalance assumption owner review pack",
    ),
    (
        "execution-semantics-master-review",
        run_execution_semantics_master_review,
        "Execution semantics master review",
    ),
    (
        "roadmap-update-after-execution-semantics-review",
        run_roadmap_update_after_execution_semantics_review,
        "Roadmap update after execution semantics review",
    ),
    (
        "reader-brief-execution-semantics-safe-preview",
        run_reader_brief_execution_semantics_safe_preview,
        "Reader Brief execution semantics safe preview",
    ),
)
