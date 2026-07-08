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
    dynamic_strategy_growth_tilt_candidate_gauntlet_harness as m2432,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_defensive_limited_adjustment_component_validation as m2434,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as m2412,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_candidate_promotion_evidence_review as m2430,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_readiness_snapshot as m2422,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_forward_outcome_binding_boundary as m2429,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_manual_review_packet_dry_run as m2427,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_observe_only_signal_artifact_boundary as m2428,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_dry_run_wiring as m2425,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_enablement_plan as m2424,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_preflight as m2423,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_schedule_dry_run as m2426,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck as m2419,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation as m2421,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_artifact_source_traceability_remediation as m2420,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as m2414,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_upstream_artifact_closure as m2417,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_valid_until_dependency_evidence_closure as m2418,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_existing_candidate_evidence_matrix as m2431,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_false_risk_off_missed_upside_batch_screen as m2433,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_valid_until_outcome_hit_rate_study as m2435,
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
    strategies_app.command("growth-tilt-engine-as-of-semantics-remediation")(
        _growth_tilt_engine_as_of_semantics_remediation_command
    )
    strategies_app.command("growth-tilt-engine-source-traceability-remediation")(
        _growth_tilt_engine_source_traceability_remediation_command
    )
    strategies_app.command("growth-tilt-engine-signal-validity-dependency-remediation")(
        _growth_tilt_engine_signal_validity_dependency_remediation_command
    )
    strategies_app.command("growth-tilt-engine-pit-gate-readiness-snapshot")(
        _growth_tilt_engine_pit_gate_readiness_snapshot_command
    )
    strategies_app.command(
        "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan"
    )(_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan_command)
    strategies_app.command(
        "growth-tilt-engine-source-traceability-upstream-artifact-closure"
    )(_growth_tilt_engine_source_traceability_upstream_artifact_closure_command)
    strategies_app.command(
        "growth-tilt-engine-valid-until-dependency-evidence-closure"
    )(_growth_tilt_engine_valid_until_dependency_evidence_closure_command)
    strategies_app.command("growth-tilt-engine-pit-gate-readiness-recheck")(
        _growth_tilt_engine_pit_gate_readiness_recheck_command
    )
    strategies_app.command(
        "growth-tilt-engine-signal-artifact-source-traceability-remediation"
    )(_growth_tilt_engine_signal_artifact_source_traceability_remediation_command)
    strategies_app.command(
        "growth-tilt-engine-pit-gate-readiness-recheck-after-source-traceability-"
        "remediation"
    )(
        _growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation_command
    )
    strategies_app.command("growth-tilt-engine-contract-readiness-snapshot")(
        _growth_tilt_engine_contract_readiness_snapshot_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-preflight")(
        _growth_tilt_engine_paper_shadow_preflight_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-enablement-plan")(
        _growth_tilt_engine_paper_shadow_enablement_plan_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-dry-run-wiring")(
        _growth_tilt_engine_paper_shadow_dry_run_wiring_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-schedule-dry-run")(
        _growth_tilt_engine_paper_shadow_schedule_dry_run_command
    )
    strategies_app.command("growth-tilt-engine-manual-review-packet-dry-run")(
        _growth_tilt_engine_manual_review_packet_dry_run_command
    )
    strategies_app.command("growth-tilt-engine-observe-only-signal-artifact-boundary")(
        _growth_tilt_engine_observe_only_signal_artifact_boundary_command
    )
    strategies_app.command("growth-tilt-engine-forward-outcome-binding-boundary")(
        _growth_tilt_engine_forward_outcome_binding_boundary_command
    )
    strategies_app.command("growth-tilt-engine-candidate-promotion-evidence-review")(
        _growth_tilt_engine_candidate_promotion_evidence_review_command
    )
    strategies_app.command("growth-tilt-existing-candidate-evidence-matrix")(
        _growth_tilt_existing_candidate_evidence_matrix_command
    )
    strategies_app.command("growth-tilt-candidate-gauntlet")(
        _growth_tilt_candidate_gauntlet_command
    )
    strategies_app.command("growth-tilt-false-risk-off-missed-upside-batch-screen")(
        _growth_tilt_false_risk_off_missed_upside_batch_screen_command
    )
    strategies_app.command(
        "growth-tilt-defensive-limited-adjustment-component-validation"
    )(_growth_tilt_defensive_limited_adjustment_component_validation_command)
    strategies_app.command("growth-tilt-valid-until-outcome-hit-rate-study")(
        _growth_tilt_valid_until_outcome_hit_rate_study_command
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


def _growth_tilt_engine_as_of_semantics_remediation_command(
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2412.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_contract_gap_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2411-contract-gap-remediation-plan")
    ] = m2412.DEFAULT_SOURCE_2411_CONTRACT_GAP_REMEDIATION_PLAN_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2412.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2412.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    source_2411_research_doc_path: Annotated[
        Path, typer.Option("--source-2411-research-doc")
    ] = m2412.DEFAULT_SOURCE_2411_RESEARCH_DOC_PATH,
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2412.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2412.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2412.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2412.run_growth_tilt_engine_as_of_semantics_remediation(
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_contract_gap_remediation_plan_path=(
            source_2411_contract_gap_remediation_plan_path
        ),
        source_2411_ordered_remediation_items_path=(
            source_2411_ordered_remediation_items_path
        ),
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        source_2411_research_doc_path=source_2411_research_doc_path,
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine as-of semantics remediation",
        payload,
    )
    for field in (
        "as_of_remediation_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "input_gap_count",
        "as_of_gap_count",
        "as_of_remediated_count",
        "remaining_blocked_or_gap_count",
        "contract_ready_count",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_source_traceability_remediation_command(
    source_2412_as_of_remediation_result_path: Annotated[
        Path, typer.Option("--source-2412-as-of-remediation-result")
    ] = m2413.DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH,
    source_2412_before_after_remediation_path: Annotated[
        Path, typer.Option("--source-2412-before-after-remediation")
    ] = m2413.DEFAULT_SOURCE_2412_BEFORE_AFTER_REMEDIATION_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2413.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2412_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2412-remaining-blocker-summary")
    ] = m2413.DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2412_research_doc_path: Annotated[
        Path, typer.Option("--source-2412-research-doc")
    ] = m2413.DEFAULT_SOURCE_2412_RESEARCH_DOC_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2413.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2413.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2413.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2413.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2413.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2413.run_growth_tilt_engine_source_traceability_remediation(
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_before_after_remediation_path=source_2412_before_after_remediation_path,
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2412_research_doc_path=source_2412_research_doc_path,
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=source_2411_ordered_remediation_items_path,
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine source traceability remediation",
        payload,
    )
    for field in (
        "source_traceability_remediation_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "input_gap_count",
        "source_traceability_gap_count",
        "source_traceability_remediated_count",
        "remaining_blocked_or_gap_count",
        "contract_ready_count",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_signal_validity_dependency_remediation_command(
    source_2413_source_traceability_remediation_result_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-remediation-result")
    ] = m2414.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH,
    source_2413_source_traceability_contract_metadata_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-contract-metadata")
    ] = m2414.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_CONTRACT_METADATA_PATH,
    source_2413_before_after_remediation_path: Annotated[
        Path, typer.Option("--source-2413-before-after-remediation")
    ] = m2414.DEFAULT_SOURCE_2413_BEFORE_AFTER_REMEDIATION_PATH,
    source_2413_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2413-updated-source-feature-mapping")
    ] = m2414.DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2413_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2413-remaining-blocker-summary")
    ] = m2414.DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2413_research_doc_path: Annotated[
        Path, typer.Option("--source-2413-research-doc")
    ] = m2414.DEFAULT_SOURCE_2413_RESEARCH_DOC_PATH,
    source_2412_as_of_remediation_result_path: Annotated[
        Path, typer.Option("--source-2412-as-of-remediation-result")
    ] = m2414.DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2414.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2412_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2412-remaining-blocker-summary")
    ] = m2414.DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2412_research_doc_path: Annotated[
        Path, typer.Option("--source-2412-research-doc")
    ] = m2414.DEFAULT_SOURCE_2412_RESEARCH_DOC_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2414.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2414.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2414.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2414.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2414.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2414.run_growth_tilt_engine_signal_validity_dependency_remediation(
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_source_traceability_contract_metadata_path=(
            source_2413_source_traceability_contract_metadata_path
        ),
        source_2413_before_after_remediation_path=(
            source_2413_before_after_remediation_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2413_research_doc_path=source_2413_research_doc_path,
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2412_research_doc_path=source_2412_research_doc_path,
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=source_2411_ordered_remediation_items_path,
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine signal validity dependency remediation",
        payload,
    )
    for field in (
        "signal_validity_dependency_remediation_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "input_gap_count",
        "validity_dependency_gap_count",
        "validity_dependency_remediated_count",
        "validity_dependency_blocked_by_valid_until_window_count",
        "validity_dependency_blocked_by_source_traceability_count",
        "remaining_blocked_or_gap_count",
        "contract_ready_count",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_readiness_snapshot_command(
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2415.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Annotated[
        Path, typer.Option("--source-2410-source-feature-contract-mapping")
    ] = m2415.DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2415.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2415.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    source_2412_as_of_remediation_result_path: Annotated[
        Path, typer.Option("--source-2412-as-of-remediation-result")
    ] = m2415.DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2415.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2412_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2412-remaining-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2413_source_traceability_remediation_result_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-remediation-result")
    ] = m2415.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH,
    source_2413_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2413-updated-source-feature-mapping")
    ] = m2415.DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2413_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2413-remaining-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2414_signal_validity_dependency_remediation_result_path: Annotated[
        Path, typer.Option("--source-2414-signal-validity-dependency-remediation-result")
    ] = m2415.DEFAULT_SOURCE_2414_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_RESULT_PATH,
    source_2414_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2414-updated-source-feature-mapping")
    ] = m2415.DEFAULT_SOURCE_2414_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2414_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2414-remaining-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2415.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2415.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2415.run_growth_tilt_engine_pit_gate_readiness_snapshot(
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=source_2411_ordered_remediation_items_path,
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2414_signal_validity_dependency_remediation_result_path=(
            source_2414_signal_validity_dependency_remediation_result_path
        ),
        source_2414_updated_source_feature_mapping_path=(
            source_2414_updated_source_feature_mapping_path
        ),
        source_2414_remaining_blocker_summary_path=(
            source_2414_remaining_blocker_summary_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate readiness snapshot",
        payload,
    )
    for field in (
        "pit_gate_readiness_snapshot_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "source_feature_count",
        "as_of_ready_count",
        "source_traceability_ready_count",
        "validity_dependency_ready_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_remaining_blocker_closure_plan_command(
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2416.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2416.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2415_readiness_validation_path: Annotated[
        Path, typer.Option("--source-2415-readiness-validation")
    ] = m2416.DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH,
    source_2415_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2415-remaining-blocker-summary")
    ] = m2416.DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2416.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2416.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2416.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2416.run_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan(
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2415_readiness_validation_path=source_2415_readiness_validation_path,
        source_2415_remaining_blocker_summary_path=(
            source_2415_remaining_blocker_summary_path
        ),
        pit_input_registry_path=pit_input_registry_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate remaining blocker closure plan",
        payload,
    )
    for field in (
        "remaining_blocker_matrix_ready",
        "source_traceability_closure_plan_ready",
        "as_of_evidence_closure_plan_ready",
        "valid_until_dependency_closure_plan_ready",
        "pit_gate_evidence_requirements_ready",
        "growth_tilt_engine_blocking_gap_resolved",
        "growth_tilt_engine_severity_downgraded",
        "valid_until_window_blocking_gap_resolved",
        "valid_until_window_severity_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
        "source_feature_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_source_traceability_upstream_artifact_closure_command(
    source_2416_closure_result_path: Annotated[
        Path, typer.Option("--source-2416-closure-result")
    ] = m2417.DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Annotated[
        Path, typer.Option("--source-2416-remaining-blocker-matrix")
    ] = m2417.DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH,
    source_2416_source_traceability_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-source-traceability-closure-plan")
    ] = m2417.DEFAULT_SOURCE_2416_SOURCE_TRACEABILITY_CLOSURE_PLAN_PATH,
    source_2416_as_of_evidence_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-as-of-evidence-closure-plan")
    ] = m2417.DEFAULT_SOURCE_2416_AS_OF_EVIDENCE_CLOSURE_PLAN_PATH,
    source_2416_valid_until_dependency_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-valid-until-dependency-closure-plan")
    ] = m2417.DEFAULT_SOURCE_2416_VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_PATH,
    source_2416_pit_gate_evidence_requirements_path: Annotated[
        Path, typer.Option("--source-2416-pit-gate-evidence-requirements")
    ] = m2417.DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH,
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2417.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2417.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2413_source_traceability_remediation_result_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-remediation-result")
    ] = m2417.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH,
    source_2413_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2413-updated-source-feature-mapping")
    ] = m2417.DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2413_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2413-remaining-blocker-summary")
    ] = m2417.DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2417.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2417.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Annotated[
        Path, typer.Option("--source-2410-source-feature-contract-mapping")
    ] = m2417.DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2417.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2417.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2417.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2417.run_growth_tilt_engine_source_traceability_upstream_artifact_closure(
            source_2416_closure_result_path=source_2416_closure_result_path,
            source_2416_remaining_blocker_matrix_path=(
                source_2416_remaining_blocker_matrix_path
            ),
            source_2416_source_traceability_closure_plan_path=(
                source_2416_source_traceability_closure_plan_path
            ),
            source_2416_as_of_evidence_closure_plan_path=(
                source_2416_as_of_evidence_closure_plan_path
            ),
            source_2416_valid_until_dependency_closure_plan_path=(
                source_2416_valid_until_dependency_closure_plan_path
            ),
            source_2416_pit_gate_evidence_requirements_path=(
                source_2416_pit_gate_evidence_requirements_path
            ),
            source_2415_readiness_snapshot_result_path=(
                source_2415_readiness_snapshot_result_path
            ),
            source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
            source_2413_source_traceability_remediation_result_path=(
                source_2413_source_traceability_remediation_result_path
            ),
            source_2413_updated_source_feature_mapping_path=(
                source_2413_updated_source_feature_mapping_path
            ),
            source_2413_remaining_blocker_summary_path=(
                source_2413_remaining_blocker_summary_path
            ),
            source_2412_updated_source_feature_mapping_path=(
                source_2412_updated_source_feature_mapping_path
            ),
            source_2410_mapping_result_path=source_2410_mapping_result_path,
            source_2410_source_feature_contract_mapping_path=(
                source_2410_source_feature_contract_mapping_path
            ),
            pit_input_registry_path=pit_input_registry_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt engine source traceability upstream artifact closure",
        payload,
    )
    for field in (
        "source_traceability_closure_evidence_ready",
        "upstream_artifact_closure_evidence_ready",
        "updated_source_feature_mapping_ready",
        "remaining_blocker_summary_ready",
        "pit_gate_recheck_required",
        "auto_mark_pit_gate_ready",
        "auto_mark_contract_ready",
        "growth_tilt_engine_blocking_gap_resolved",
        "growth_tilt_engine_severity_downgraded",
        "valid_until_window_blocking_gap_resolved",
        "valid_until_window_severity_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
        "source_feature_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
        "source_traceability_evidence_row_count",
        "source_traceability_pre_recheck_evidence_ready_count",
        "source_traceability_still_blocked_count",
        "upstream_artifact_closure_evidence_row_count",
        "upstream_artifact_pre_recheck_evidence_ready_count",
        "upstream_artifact_still_blocked_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_valid_until_dependency_evidence_closure_command(
    source_2417_closure_result_path: Annotated[
        Path, typer.Option("--source-2417-closure-result")
    ] = m2418.DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH,
    source_2417_source_traceability_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-source-traceability-closure-evidence")
    ] = m2418.DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH,
    source_2417_upstream_artifact_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-upstream-artifact-closure-evidence")
    ] = m2418.DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH,
    source_2417_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2417-updated-source-feature-mapping")
    ] = m2418.DEFAULT_SOURCE_2417_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2417_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2417-remaining-blocker-summary")
    ] = m2418.DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2416_closure_result_path: Annotated[
        Path, typer.Option("--source-2416-closure-result")
    ] = m2418.DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Annotated[
        Path, typer.Option("--source-2416-remaining-blocker-matrix")
    ] = m2418.DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH,
    source_2416_valid_until_dependency_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-valid-until-dependency-closure-plan")
    ] = m2418.DEFAULT_SOURCE_2416_VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_PATH,
    source_2416_pit_gate_evidence_requirements_path: Annotated[
        Path, typer.Option("--source-2416-pit-gate-evidence-requirements")
    ] = m2418.DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH,
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2418.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2418.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2414_remediation_result_path: Annotated[
        Path, typer.Option("--source-2414-remediation-result")
    ] = m2418.DEFAULT_SOURCE_2414_REMEDIATION_RESULT_PATH,
    source_2414_contract_metadata_path: Annotated[
        Path, typer.Option("--source-2414-contract-metadata")
    ] = m2418.DEFAULT_SOURCE_2414_CONTRACT_METADATA_PATH,
    source_2414_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2414-remaining-blocker-summary")
    ] = m2418.DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2418.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2407_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2407-remediation-plan-result")
    ] = m2418.DEFAULT_SOURCE_2407_REMEDIATION_PLAN_RESULT_PATH,
    source_2407_valid_until_semantics_review_path: Annotated[
        Path, typer.Option("--source-2407-valid-until-semantics-review")
    ] = m2418.DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH,
    source_2407_stale_signal_risk_audit_path: Annotated[
        Path, typer.Option("--source-2407-stale-signal-risk-audit")
    ] = m2418.DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH,
    source_2407_signal_validity_contract_plan_path: Annotated[
        Path, typer.Option("--source-2407-signal-validity-contract-plan")
    ] = m2418.DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH,
    source_2407_validation_plan_path: Annotated[
        Path, typer.Option("--source-2407-validation-plan")
    ] = m2418.DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2418.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    strategy_execution_policy_registry_path: Annotated[
        Path, typer.Option("--strategy-execution-policy-registry")
    ] = m2418.DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2418.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2418.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2418.run_growth_tilt_engine_valid_until_dependency_evidence_closure(
        source_2417_closure_result_path=source_2417_closure_result_path,
        source_2417_source_traceability_closure_evidence_path=(
            source_2417_source_traceability_closure_evidence_path
        ),
        source_2417_upstream_artifact_closure_evidence_path=(
            source_2417_upstream_artifact_closure_evidence_path
        ),
        source_2417_updated_source_feature_mapping_path=(
            source_2417_updated_source_feature_mapping_path
        ),
        source_2417_remaining_blocker_summary_path=(
            source_2417_remaining_blocker_summary_path
        ),
        source_2416_closure_result_path=source_2416_closure_result_path,
        source_2416_remaining_blocker_matrix_path=(
            source_2416_remaining_blocker_matrix_path
        ),
        source_2416_valid_until_dependency_closure_plan_path=(
            source_2416_valid_until_dependency_closure_plan_path
        ),
        source_2416_pit_gate_evidence_requirements_path=(
            source_2416_pit_gate_evidence_requirements_path
        ),
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2414_remediation_result_path=source_2414_remediation_result_path,
        source_2414_contract_metadata_path=source_2414_contract_metadata_path,
        source_2414_remaining_blocker_summary_path=(
            source_2414_remaining_blocker_summary_path
        ),
        source_2411_remediation_plan_result_path=(
            source_2411_remediation_plan_result_path
        ),
        source_2407_remediation_plan_result_path=(
            source_2407_remediation_plan_result_path
        ),
        source_2407_valid_until_semantics_review_path=(
            source_2407_valid_until_semantics_review_path
        ),
        source_2407_stale_signal_risk_audit_path=(
            source_2407_stale_signal_risk_audit_path
        ),
        source_2407_signal_validity_contract_plan_path=(
            source_2407_signal_validity_contract_plan_path
        ),
        source_2407_validation_plan_path=source_2407_validation_plan_path,
        pit_input_registry_path=pit_input_registry_path,
        strategy_execution_policy_registry_path=(
            strategy_execution_policy_registry_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine valid-until dependency evidence closure",
        payload,
    )
    for field in (
        "valid_until_dependency_evidence_ready",
        "signal_validity_contract_evidence_ready",
        "stale_signal_policy_evidence_ready",
        "growth_tilt_valid_until_alignment_evidence_ready",
        "remaining_blocker_summary_ready",
        "pit_gate_recheck_required",
        "auto_mark_pit_gate_ready",
        "auto_mark_contract_ready",
        "auto_downgrade_blocker",
        "growth_tilt_engine_blocking_gap_resolved",
        "growth_tilt_engine_severity_downgraded",
        "valid_until_window_blocking_gap_resolved",
        "valid_until_window_severity_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
        "source_feature_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
        "valid_until_window_dependency_blocker_count_from_2415",
        "valid_until_dependency_evidence_row_count",
        "valid_until_dependency_pre_recheck_evidence_ready_count",
        "valid_until_dependency_still_blocked_count",
        "source_traceability_still_blocked",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_readiness_recheck_command(
    source_2418_closure_result_path: Annotated[
        Path, typer.Option("--source-2418-closure-result")
    ] = m2419.DEFAULT_SOURCE_2418_CLOSURE_RESULT_PATH,
    source_2418_valid_until_dependency_evidence_path: Annotated[
        Path, typer.Option("--source-2418-valid-until-dependency-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_VALID_UNTIL_DEPENDENCY_EVIDENCE_PATH,
    source_2418_signal_validity_contract_evidence_path: Annotated[
        Path, typer.Option("--source-2418-signal-validity-contract-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_SIGNAL_VALIDITY_CONTRACT_EVIDENCE_PATH,
    source_2418_stale_signal_policy_evidence_path: Annotated[
        Path, typer.Option("--source-2418-stale-signal-policy-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_EVIDENCE_PATH,
    source_2418_growth_tilt_valid_until_alignment_evidence_path: Annotated[
        Path, typer.Option("--source-2418-growth-tilt-valid-until-alignment-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_PATH,
    source_2418_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2418-remaining-blocker-summary")
    ] = m2419.DEFAULT_SOURCE_2418_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2418_research_doc_path: Annotated[
        Path, typer.Option("--source-2418-research-doc")
    ] = m2419.DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH,
    source_2418_route_doc_path: Annotated[
        Path, typer.Option("--source-2418-route-doc")
    ] = m2419.DEFAULT_SOURCE_2418_ROUTE_DOC_PATH,
    source_2417_closure_result_path: Annotated[
        Path, typer.Option("--source-2417-closure-result")
    ] = m2419.DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH,
    source_2417_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2417-remaining-blocker-summary")
    ] = m2419.DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2416_closure_result_path: Annotated[
        Path, typer.Option("--source-2416-closure-result")
    ] = m2419.DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Annotated[
        Path, typer.Option("--source-2416-remaining-blocker-matrix")
    ] = m2419.DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH,
    source_2416_pit_gate_evidence_requirements_path: Annotated[
        Path, typer.Option("--source-2416-pit-gate-evidence-requirements")
    ] = m2419.DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH,
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2419.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2419.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2415_readiness_validation_path: Annotated[
        Path, typer.Option("--source-2415-readiness-validation")
    ] = m2419.DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH,
    source_2415_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2415-remaining-blocker-summary")
    ] = m2419.DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2419.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2419.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2419.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2419.run_growth_tilt_engine_pit_gate_readiness_recheck(
        source_2418_closure_result_path=source_2418_closure_result_path,
        source_2418_valid_until_dependency_evidence_path=(
            source_2418_valid_until_dependency_evidence_path
        ),
        source_2418_signal_validity_contract_evidence_path=(
            source_2418_signal_validity_contract_evidence_path
        ),
        source_2418_stale_signal_policy_evidence_path=(
            source_2418_stale_signal_policy_evidence_path
        ),
        source_2418_growth_tilt_valid_until_alignment_evidence_path=(
            source_2418_growth_tilt_valid_until_alignment_evidence_path
        ),
        source_2418_remaining_blocker_summary_path=(
            source_2418_remaining_blocker_summary_path
        ),
        source_2418_research_doc_path=source_2418_research_doc_path,
        source_2418_route_doc_path=source_2418_route_doc_path,
        source_2417_closure_result_path=source_2417_closure_result_path,
        source_2417_remaining_blocker_summary_path=(
            source_2417_remaining_blocker_summary_path
        ),
        source_2416_closure_result_path=source_2416_closure_result_path,
        source_2416_remaining_blocker_matrix_path=(
            source_2416_remaining_blocker_matrix_path
        ),
        source_2416_pit_gate_evidence_requirements_path=(
            source_2416_pit_gate_evidence_requirements_path
        ),
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2415_readiness_validation_path=(
            source_2415_readiness_validation_path
        ),
        source_2415_remaining_blocker_summary_path=(
            source_2415_remaining_blocker_summary_path
        ),
        pit_input_registry_path=pit_input_registry_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate readiness recheck",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "remaining_blocker_count",
        "remaining_blockers",
        "blocker_classification",
        "valid_until_dependency_evidence_ready_from_2418",
        "valid_until_dependency_still_blocked_count_after_recheck",
        "auto_mark_pit_gate_ready",
        "auto_mark_contract_ready",
        "auto_downgrade_blocker",
        "blockers_resolved",
        "blockers_downgraded",
        "signal_artifact_source_traceability_blocker_resolved",
        "signal_artifact_source_traceability_blocker_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_signal_artifact_source_traceability_remediation_command(
    source_2419_recheck_result_path: Annotated[
        Path, typer.Option("--source-2419-recheck-result")
    ] = m2420.DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH,
    source_2419_blocker_classification_path: Annotated[
        Path, typer.Option("--source-2419-blocker-classification")
    ] = m2420.DEFAULT_SOURCE_2419_BLOCKER_CLASSIFICATION_PATH,
    source_2419_research_doc_path: Annotated[
        Path, typer.Option("--source-2419-research-doc")
    ] = m2420.DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH,
    source_2419_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2419-blocker-doc")
    ] = m2420.DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH,
    source_2418_valid_until_dependency_evidence_path: Annotated[
        Path, typer.Option("--source-2418-valid-until-dependency-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_VALID_UNTIL_DEPENDENCY_EVIDENCE_PATH,
    source_2418_signal_validity_contract_evidence_path: Annotated[
        Path, typer.Option("--source-2418-signal-validity-contract-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_SIGNAL_VALIDITY_CONTRACT_EVIDENCE_PATH,
    source_2418_stale_signal_policy_evidence_path: Annotated[
        Path, typer.Option("--source-2418-stale-signal-policy-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_EVIDENCE_PATH,
    source_2418_growth_tilt_valid_until_alignment_evidence_path: Annotated[
        Path, typer.Option("--source-2418-growth-tilt-valid-until-alignment-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_PATH,
    source_2418_research_doc_path: Annotated[
        Path, typer.Option("--source-2418-research-doc")
    ] = m2420.DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH,
    source_2417_source_traceability_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-source-traceability-closure-evidence")
    ] = m2420.DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH,
    source_2417_upstream_artifact_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-upstream-artifact-closure-evidence")
    ] = m2420.DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH,
    source_2417_source_traceability_doc_path: Annotated[
        Path, typer.Option("--source-2417-source-traceability-doc")
    ] = m2420.DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_DOC_PATH,
    source_2417_upstream_artifact_doc_path: Annotated[
        Path, typer.Option("--source-2417-upstream-artifact-doc")
    ] = m2420.DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2420.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2420.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2420.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2420.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2420.run_growth_tilt_engine_signal_artifact_source_traceability_remediation(
        source_2419_recheck_result_path=source_2419_recheck_result_path,
        source_2419_blocker_classification_path=(
            source_2419_blocker_classification_path
        ),
        source_2419_research_doc_path=source_2419_research_doc_path,
        source_2419_blocker_doc_path=source_2419_blocker_doc_path,
        source_2418_valid_until_dependency_evidence_path=(
            source_2418_valid_until_dependency_evidence_path
        ),
        source_2418_signal_validity_contract_evidence_path=(
            source_2418_signal_validity_contract_evidence_path
        ),
        source_2418_stale_signal_policy_evidence_path=(
            source_2418_stale_signal_policy_evidence_path
        ),
        source_2418_growth_tilt_valid_until_alignment_evidence_path=(
            source_2418_growth_tilt_valid_until_alignment_evidence_path
        ),
        source_2418_research_doc_path=source_2418_research_doc_path,
        source_2417_source_traceability_closure_evidence_path=(
            source_2417_source_traceability_closure_evidence_path
        ),
        source_2417_upstream_artifact_closure_evidence_path=(
            source_2417_upstream_artifact_closure_evidence_path
        ),
        source_2417_source_traceability_doc_path=(
            source_2417_source_traceability_doc_path
        ),
        source_2417_upstream_artifact_doc_path=(
            source_2417_upstream_artifact_doc_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine signal artifact source traceability remediation",
        payload,
    )
    for field in (
        "remediation_status",
        "artifact_id",
        "source_traceability_evidence_complete",
        "source_traceability_blocker_resolved",
        "blocker_resolved",
        "blocker_downgraded",
        "pit_gate_ready",
        "contract_ready",
        "pit_gate_ready_count",
        "contract_ready_count",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    missing = payload.get("missing_source_evidence_summary") or {}
    if isinstance(missing, dict):
        console.print(f"missing_field_count={missing.get('missing_field_count')}")
        console.print(f"incomplete_field_count={missing.get('incomplete_field_count')}")
        console.print(f"unresolved_blocker_count={missing.get('unresolved_blocker_count')}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation_command(
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2421.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2421.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2421.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2421.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2421.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2421.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2421.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2421.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    source_2419_recheck_result_path: Annotated[
        Path, typer.Option("--source-2419-recheck-result")
    ] = m2421.DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH,
    source_2419_research_doc_path: Annotated[
        Path, typer.Option("--source-2419-research-doc")
    ] = m2421.DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH,
    source_2419_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2419-blocker-doc")
    ] = m2421.DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2421.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2421.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2421.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2421.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2421.run_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation(
            source_2420_remediation_result_path=source_2420_remediation_result_path,
            source_2420_source_traceability_manifest_path=(
                source_2420_source_traceability_manifest_path
            ),
            source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
            source_2420_missing_source_evidence_summary_path=(
                source_2420_missing_source_evidence_summary_path
            ),
            source_2420_research_doc_path=source_2420_research_doc_path,
            source_2420_manifest_doc_path=source_2420_manifest_doc_path,
            source_2420_lineage_doc_path=source_2420_lineage_doc_path,
            source_2420_route_doc_path=source_2420_route_doc_path,
            source_2419_recheck_result_path=source_2419_recheck_result_path,
            source_2419_research_doc_path=source_2419_research_doc_path,
            source_2419_blocker_doc_path=source_2419_blocker_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate readiness recheck after source traceability remediation",
        payload,
    )
    for field in (
        "readiness_status",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_evidence_complete_after_2420",
        "source_traceability_blocker_resolved",
        "signal_artifact_source_traceability_blocker_resolved",
        "blockers_resolved",
        "blockers_downgraded",
        "resolved_blockers",
        "remaining_blockers",
        "remaining_blocker_count",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "pit_gate_blocked_count",
        "contract_ready",
        "contract_ready_count",
        "contract_readiness_snapshot_required",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
        "blocker_resolution_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_contract_readiness_snapshot_command(
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2422.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_pit_gate_recheck_matrix_path: Annotated[
        Path, typer.Option("--source-2421-pit-gate-recheck-matrix")
    ] = m2422.DEFAULT_SOURCE_2421_PIT_GATE_RECHECK_MATRIX_PATH,
    source_2421_blocker_resolution_summary_path: Annotated[
        Path, typer.Option("--source-2421-blocker-resolution-summary")
    ] = m2422.DEFAULT_SOURCE_2421_BLOCKER_RESOLUTION_SUMMARY_PATH,
    source_2421_contract_readiness_snapshot_gate_path: Annotated[
        Path, typer.Option("--source-2421-contract-readiness-snapshot-gate")
    ] = m2422.DEFAULT_SOURCE_2421_CONTRACT_READINESS_SNAPSHOT_GATE_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2422.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_matrix_doc_path: Annotated[
        Path, typer.Option("--source-2421-matrix-doc")
    ] = m2422.DEFAULT_SOURCE_2421_MATRIX_DOC_PATH,
    source_2421_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2421-blocker-doc")
    ] = m2422.DEFAULT_SOURCE_2421_BLOCKER_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2422.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2422.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2422.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2422.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2422.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2422.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2422.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2422.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2422.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2422.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2422.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2422.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2422.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2422.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2422.run_growth_tilt_engine_contract_readiness_snapshot(
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_pit_gate_recheck_matrix_path=(
            source_2421_pit_gate_recheck_matrix_path
        ),
        source_2421_blocker_resolution_summary_path=(
            source_2421_blocker_resolution_summary_path
        ),
        source_2421_contract_readiness_snapshot_gate_path=(
            source_2421_contract_readiness_snapshot_gate_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_matrix_doc_path=source_2421_matrix_doc_path,
        source_2421_blocker_doc_path=source_2421_blocker_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine contract readiness snapshot",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "pit_gate_blocked_count",
        "remaining_blockers",
        "remaining_blocker_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_evidence_complete_after_2420",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "missing_contract_evidence_count",
        "incomplete_contract_field_count",
        "contract_requirement_count",
        "contract_requirement_pass_count",
        "contract_requirement_fail_count",
        "paper_shadow_preflight_required",
        "paper_shadow_preflight_started",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_preflight_command(
    source_2422_contract_readiness_snapshot_path: Annotated[
        Path, typer.Option("--source-2422-contract-readiness-snapshot")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH,
    source_2422_contract_evidence_map_path: Annotated[
        Path, typer.Option("--source-2422-contract-evidence-map")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_EVIDENCE_MAP_PATH,
    source_2422_contract_gap_summary_path: Annotated[
        Path, typer.Option("--source-2422-contract-gap-summary")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_GAP_SUMMARY_PATH,
    source_2422_contract_requirements_path: Annotated[
        Path, typer.Option("--source-2422-contract-requirements")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_REQUIREMENTS_PATH,
    source_2422_research_doc_path: Annotated[
        Path, typer.Option("--source-2422-research-doc")
    ] = m2423.DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_evidence_map_doc_path: Annotated[
        Path, typer.Option("--source-2422-evidence-map-doc")
    ] = m2423.DEFAULT_SOURCE_2422_EVIDENCE_MAP_DOC_PATH,
    source_2422_gap_summary_doc_path: Annotated[
        Path, typer.Option("--source-2422-gap-summary-doc")
    ] = m2423.DEFAULT_SOURCE_2422_GAP_SUMMARY_DOC_PATH,
    source_2422_route_doc_path: Annotated[
        Path, typer.Option("--source-2422-route-doc")
    ] = m2423.DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2423.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_pit_gate_recheck_matrix_path: Annotated[
        Path, typer.Option("--source-2421-pit-gate-recheck-matrix")
    ] = m2423.DEFAULT_SOURCE_2421_PIT_GATE_RECHECK_MATRIX_PATH,
    source_2421_blocker_resolution_summary_path: Annotated[
        Path, typer.Option("--source-2421-blocker-resolution-summary")
    ] = m2423.DEFAULT_SOURCE_2421_BLOCKER_RESOLUTION_SUMMARY_PATH,
    source_2421_contract_readiness_snapshot_gate_path: Annotated[
        Path, typer.Option("--source-2421-contract-readiness-snapshot-gate")
    ] = m2423.DEFAULT_SOURCE_2421_CONTRACT_READINESS_SNAPSHOT_GATE_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2423.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_matrix_doc_path: Annotated[
        Path, typer.Option("--source-2421-matrix-doc")
    ] = m2423.DEFAULT_SOURCE_2421_MATRIX_DOC_PATH,
    source_2421_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2421-blocker-doc")
    ] = m2423.DEFAULT_SOURCE_2421_BLOCKER_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2423.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2423.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2423.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2423.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2423.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2423.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2423.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2423.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2423.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2423.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2423.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2423.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2423.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2423.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2423.run_growth_tilt_engine_paper_shadow_preflight(
        source_2422_contract_readiness_snapshot_path=(
            source_2422_contract_readiness_snapshot_path
        ),
        source_2422_contract_evidence_map_path=source_2422_contract_evidence_map_path,
        source_2422_contract_gap_summary_path=source_2422_contract_gap_summary_path,
        source_2422_contract_requirements_path=source_2422_contract_requirements_path,
        source_2422_research_doc_path=source_2422_research_doc_path,
        source_2422_evidence_map_doc_path=source_2422_evidence_map_doc_path,
        source_2422_gap_summary_doc_path=source_2422_gap_summary_doc_path,
        source_2422_route_doc_path=source_2422_route_doc_path,
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_pit_gate_recheck_matrix_path=(
            source_2421_pit_gate_recheck_matrix_path
        ),
        source_2421_blocker_resolution_summary_path=(
            source_2421_blocker_resolution_summary_path
        ),
        source_2421_contract_readiness_snapshot_gate_path=(
            source_2421_contract_readiness_snapshot_gate_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_matrix_doc_path=source_2421_matrix_doc_path,
        source_2421_blocker_doc_path=source_2421_blocker_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow preflight",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "remaining_pit_blockers",
        "remaining_pit_blocker_count",
        "contract_readiness_status",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_accepted",
        "paper_shadow_preflight_started",
        "paper_shadow_preflight_completed",
        "paper_shadow_preflight_ready",
        "preflight_gap_count",
        "missing_preflight_evidence_count",
        "safety_boundary_gap_count",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_enablement_plan_command(
    source_2423_preflight_result_path: Annotated[
        Path, typer.Option("--source-2423-preflight-result")
    ] = m2424.DEFAULT_SOURCE_2423_PREFLIGHT_RESULT_PATH,
    source_2423_preflight_checklist_path: Annotated[
        Path, typer.Option("--source-2423-preflight-checklist")
    ] = m2424.DEFAULT_SOURCE_2423_PREFLIGHT_CHECKLIST_PATH,
    source_2423_preflight_gap_summary_path: Annotated[
        Path, typer.Option("--source-2423-preflight-gap-summary")
    ] = m2424.DEFAULT_SOURCE_2423_PREFLIGHT_GAP_SUMMARY_PATH,
    source_2423_research_doc_path: Annotated[
        Path, typer.Option("--source-2423-research-doc")
    ] = m2424.DEFAULT_SOURCE_2423_RESEARCH_DOC_PATH,
    source_2423_checklist_doc_path: Annotated[
        Path, typer.Option("--source-2423-checklist-doc")
    ] = m2424.DEFAULT_SOURCE_2423_CHECKLIST_DOC_PATH,
    source_2423_gap_summary_doc_path: Annotated[
        Path, typer.Option("--source-2423-gap-summary-doc")
    ] = m2424.DEFAULT_SOURCE_2423_GAP_SUMMARY_DOC_PATH,
    source_2423_route_doc_path: Annotated[
        Path, typer.Option("--source-2423-route-doc")
    ] = m2424.DEFAULT_SOURCE_2423_ROUTE_DOC_PATH,
    source_2422_contract_readiness_snapshot_path: Annotated[
        Path, typer.Option("--source-2422-contract-readiness-snapshot")
    ] = m2424.DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH,
    source_2422_research_doc_path: Annotated[
        Path, typer.Option("--source-2422-research-doc")
    ] = m2424.DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_route_doc_path: Annotated[
        Path, typer.Option("--source-2422-route-doc")
    ] = m2424.DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2424.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2424.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2424.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2424.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2424.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2424.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2424.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2424.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2424.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2424.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2424.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2424.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2424.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2424.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2424.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2424.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2424.run_growth_tilt_engine_paper_shadow_enablement_plan(
        source_2423_preflight_result_path=source_2423_preflight_result_path,
        source_2423_preflight_checklist_path=source_2423_preflight_checklist_path,
        source_2423_preflight_gap_summary_path=(
            source_2423_preflight_gap_summary_path
        ),
        source_2423_research_doc_path=source_2423_research_doc_path,
        source_2423_checklist_doc_path=source_2423_checklist_doc_path,
        source_2423_gap_summary_doc_path=source_2423_gap_summary_doc_path,
        source_2423_route_doc_path=source_2423_route_doc_path,
        source_2422_contract_readiness_snapshot_path=(
            source_2422_contract_readiness_snapshot_path
        ),
        source_2422_research_doc_path=source_2422_research_doc_path,
        source_2422_route_doc_path=source_2422_route_doc_path,
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow enablement plan",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "remaining_pit_blockers",
        "remaining_pit_blocker_count",
        "contract_readiness_status",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_accepted",
        "paper_shadow_preflight_ready",
        "paper_shadow_enablement_plan_started",
        "paper_shadow_enablement_plan_completed",
        "enablement_plan_ready",
        "enablement_gap_count",
        "missing_enablement_evidence_count",
        "safety_boundary_gap_count",
        "preflight_or_contract_gap_count",
        "dry_run_wiring_allowed",
        "paper_shadow_schedule_dry_run_allowed",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "production_enabled",
        "broker_enabled",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "daily_report_run",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_dry_run_wiring_command(
    source_2424_enablement_plan_result_path: Annotated[
        Path, typer.Option("--source-2424-enablement-plan-result")
    ] = m2425.DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH,
    source_2424_enablement_plan_path: Annotated[
        Path, typer.Option("--source-2424-enablement-plan")
    ] = m2425.DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_PATH,
    source_2424_runtime_boundary_checklist_path: Annotated[
        Path, typer.Option("--source-2424-runtime-boundary-checklist")
    ] = m2425.DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_CHECKLIST_PATH,
    source_2424_schedule_boundary_plan_path: Annotated[
        Path, typer.Option("--source-2424-schedule-boundary-plan")
    ] = m2425.DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_PLAN_PATH,
    source_2424_manual_review_checklist_path: Annotated[
        Path, typer.Option("--source-2424-manual-review-checklist")
    ] = m2425.DEFAULT_SOURCE_2424_MANUAL_REVIEW_CHECKLIST_PATH,
    source_2424_rollback_stop_condition_summary_path: Annotated[
        Path, typer.Option("--source-2424-rollback-stop-condition-summary")
    ] = m2425.DEFAULT_SOURCE_2424_ROLLBACK_STOP_CONDITION_SUMMARY_PATH,
    source_2424_research_doc_path: Annotated[
        Path, typer.Option("--source-2424-research-doc")
    ] = m2425.DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH,
    source_2424_runtime_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2424-runtime-boundary-doc")
    ] = m2425.DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_DOC_PATH,
    source_2424_schedule_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2424-schedule-boundary-doc")
    ] = m2425.DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_DOC_PATH,
    source_2424_manual_review_doc_path: Annotated[
        Path, typer.Option("--source-2424-manual-review-doc")
    ] = m2425.DEFAULT_SOURCE_2424_MANUAL_REVIEW_DOC_PATH,
    source_2424_rollback_doc_path: Annotated[
        Path, typer.Option("--source-2424-rollback-doc")
    ] = m2425.DEFAULT_SOURCE_2424_ROLLBACK_DOC_PATH,
    source_2424_route_doc_path: Annotated[
        Path, typer.Option("--source-2424-route-doc")
    ] = m2425.DEFAULT_SOURCE_2424_ROUTE_DOC_PATH,
    source_2423_preflight_result_path: Annotated[
        Path, typer.Option("--source-2423-preflight-result")
    ] = m2425.DEFAULT_SOURCE_2423_PREFLIGHT_RESULT_PATH,
    source_2423_research_doc_path: Annotated[
        Path, typer.Option("--source-2423-research-doc")
    ] = m2425.DEFAULT_SOURCE_2423_RESEARCH_DOC_PATH,
    source_2423_route_doc_path: Annotated[
        Path, typer.Option("--source-2423-route-doc")
    ] = m2425.DEFAULT_SOURCE_2423_ROUTE_DOC_PATH,
    source_2422_contract_readiness_snapshot_path: Annotated[
        Path, typer.Option("--source-2422-contract-readiness-snapshot")
    ] = m2425.DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH,
    source_2422_research_doc_path: Annotated[
        Path, typer.Option("--source-2422-research-doc")
    ] = m2425.DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_route_doc_path: Annotated[
        Path, typer.Option("--source-2422-route-doc")
    ] = m2425.DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2425.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2425.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2425.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2425.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2425.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2425.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2425.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2425.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2425.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2425.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2425.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2425.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2425.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2425.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2425.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2425.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2425.run_growth_tilt_engine_paper_shadow_dry_run_wiring(
        source_2424_enablement_plan_result_path=(
            source_2424_enablement_plan_result_path
        ),
        source_2424_enablement_plan_path=source_2424_enablement_plan_path,
        source_2424_runtime_boundary_checklist_path=(
            source_2424_runtime_boundary_checklist_path
        ),
        source_2424_schedule_boundary_plan_path=(
            source_2424_schedule_boundary_plan_path
        ),
        source_2424_manual_review_checklist_path=(
            source_2424_manual_review_checklist_path
        ),
        source_2424_rollback_stop_condition_summary_path=(
            source_2424_rollback_stop_condition_summary_path
        ),
        source_2424_research_doc_path=source_2424_research_doc_path,
        source_2424_runtime_boundary_doc_path=source_2424_runtime_boundary_doc_path,
        source_2424_schedule_boundary_doc_path=source_2424_schedule_boundary_doc_path,
        source_2424_manual_review_doc_path=source_2424_manual_review_doc_path,
        source_2424_rollback_doc_path=source_2424_rollback_doc_path,
        source_2424_route_doc_path=source_2424_route_doc_path,
        source_2423_preflight_result_path=source_2423_preflight_result_path,
        source_2423_research_doc_path=source_2423_research_doc_path,
        source_2423_route_doc_path=source_2423_route_doc_path,
        source_2422_contract_readiness_snapshot_path=(
            source_2422_contract_readiness_snapshot_path
        ),
        source_2422_research_doc_path=source_2422_research_doc_path,
        source_2422_route_doc_path=source_2422_route_doc_path,
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow dry-run wiring",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "remaining_pit_blockers",
        "remaining_pit_blocker_count",
        "contract_readiness_status",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_accepted",
        "paper_shadow_preflight_ready",
        "enablement_plan_ready",
        "enablement_gap_count",
        "paper_shadow_dry_run_wiring_started",
        "paper_shadow_dry_run_wiring_completed",
        "dry_run_wiring_ready",
        "dry_run_wiring_gap_count",
        "missing_dry_run_evidence_count",
        "safety_boundary_gap_count",
        "wiring_contract_gap_count",
        "precondition_gap_count",
        "input_contract_map_ready",
        "output_artifact_contract_map_ready",
        "manual_review_handoff_wired",
        "schedule_hook_verified_disabled",
        "no_effect_audit_ready",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "daily_report_run",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_schedule_dry_run_command(
    source_2425_dry_run_wiring_result_path: Annotated[
        Path, typer.Option("--source-2425-dry-run-wiring-result")
    ] = m2426.DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH,
    source_2425_schedule_hook_disabled_verification_path: Annotated[
        Path, typer.Option("--source-2425-schedule-hook-disabled-verification")
    ] = m2426.DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DISABLED_VERIFICATION_PATH,
    source_2425_runtime_boundary_manifest_path: Annotated[
        Path, typer.Option("--source-2425-runtime-boundary-manifest")
    ] = m2426.DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_MANIFEST_PATH,
    source_2425_manual_review_handoff_wiring_plan_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-handoff-wiring-plan")
    ] = m2426.DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH,
    source_2425_dry_run_no_effect_audit_summary_path: Annotated[
        Path, typer.Option("--source-2425-dry-run-no-effect-audit-summary")
    ] = m2426.DEFAULT_SOURCE_2425_DRY_RUN_NO_EFFECT_AUDIT_SUMMARY_PATH,
    source_2425_research_doc_path: Annotated[
        Path, typer.Option("--source-2425-research-doc")
    ] = m2426.DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH,
    source_2425_schedule_hook_doc_path: Annotated[
        Path, typer.Option("--source-2425-schedule-hook-doc")
    ] = m2426.DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DOC_PATH,
    source_2425_runtime_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2425-runtime-boundary-doc")
    ] = m2426.DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_DOC_PATH,
    source_2425_manual_review_doc_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-doc")
    ] = m2426.DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH,
    source_2425_no_effect_audit_doc_path: Annotated[
        Path, typer.Option("--source-2425-no-effect-audit-doc")
    ] = m2426.DEFAULT_SOURCE_2425_NO_EFFECT_AUDIT_DOC_PATH,
    source_2425_route_doc_path: Annotated[
        Path, typer.Option("--source-2425-route-doc")
    ] = m2426.DEFAULT_SOURCE_2425_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2426.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2426.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2426.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2426.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2426.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2426.run_growth_tilt_engine_paper_shadow_schedule_dry_run(
        source_2425_dry_run_wiring_result_path=(
            source_2425_dry_run_wiring_result_path
        ),
        source_2425_schedule_hook_disabled_verification_path=(
            source_2425_schedule_hook_disabled_verification_path
        ),
        source_2425_runtime_boundary_manifest_path=(
            source_2425_runtime_boundary_manifest_path
        ),
        source_2425_manual_review_handoff_wiring_plan_path=(
            source_2425_manual_review_handoff_wiring_plan_path
        ),
        source_2425_dry_run_no_effect_audit_summary_path=(
            source_2425_dry_run_no_effect_audit_summary_path
        ),
        source_2425_research_doc_path=source_2425_research_doc_path,
        source_2425_schedule_hook_doc_path=source_2425_schedule_hook_doc_path,
        source_2425_runtime_boundary_doc_path=source_2425_runtime_boundary_doc_path,
        source_2425_manual_review_doc_path=source_2425_manual_review_doc_path,
        source_2425_no_effect_audit_doc_path=source_2425_no_effect_audit_doc_path,
        source_2425_route_doc_path=source_2425_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow schedule dry-run",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "paper_shadow_dry_run_wiring_status",
        "paper_shadow_dry_run_wiring_ready",
        "dry_run_wiring_gap_count",
        "schedule_hook_verified_disabled",
        "runtime_boundary_verified",
        "manual_review_handoff_wired",
        "prior_no_effect_audit_ready",
        "paper_shadow_schedule_dry_run_started",
        "paper_shadow_schedule_dry_run_completed",
        "paper_shadow_schedule_dry_run_ready",
        "schedule_dry_run_plan_ready",
        "schedule_boundary_checklist_ready",
        "schedule_no_effect_audit_ready",
        "schedule_dry_run_gap_count",
        "missing_schedule_evidence_count",
        "safety_boundary_gap_count",
        "schedule_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "schedule_hook_invoked",
        "schedule_state_mutated",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "daily_report_run",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_manual_review_packet_dry_run_command(
    source_2426_schedule_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2426-schedule-dry-run-result")
    ] = m2427.DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH,
    source_2426_schedule_boundary_checklist_path: Annotated[
        Path, typer.Option("--source-2426-schedule-boundary-checklist")
    ] = m2427.DEFAULT_SOURCE_2426_SCHEDULE_BOUNDARY_CHECKLIST_PATH,
    source_2426_schedule_no_effect_audit_summary_path: Annotated[
        Path, typer.Option("--source-2426-schedule-no-effect-audit-summary")
    ] = m2427.DEFAULT_SOURCE_2426_SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_PATH,
    source_2426_research_doc_path: Annotated[
        Path, typer.Option("--source-2426-research-doc")
    ] = m2427.DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH,
    source_2426_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2426-boundary-doc")
    ] = m2427.DEFAULT_SOURCE_2426_BOUNDARY_DOC_PATH,
    source_2426_no_effect_doc_path: Annotated[
        Path, typer.Option("--source-2426-no-effect-doc")
    ] = m2427.DEFAULT_SOURCE_2426_NO_EFFECT_DOC_PATH,
    source_2426_route_doc_path: Annotated[
        Path, typer.Option("--source-2426-route-doc")
    ] = m2427.DEFAULT_SOURCE_2426_ROUTE_DOC_PATH,
    source_2425_dry_run_wiring_result_path: Annotated[
        Path, typer.Option("--source-2425-dry-run-wiring-result")
    ] = m2427.DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH,
    source_2425_manual_review_handoff_wiring_plan_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-handoff-wiring-plan")
    ] = m2427.DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH,
    source_2425_research_doc_path: Annotated[
        Path, typer.Option("--source-2425-research-doc")
    ] = m2427.DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH,
    source_2425_manual_review_doc_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-doc")
    ] = m2427.DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH,
    source_2425_route_doc_path: Annotated[
        Path, typer.Option("--source-2425-route-doc")
    ] = m2427.DEFAULT_SOURCE_2425_ROUTE_DOC_PATH,
    source_2424_enablement_plan_result_path: Annotated[
        Path, typer.Option("--source-2424-enablement-plan-result")
    ] = m2427.DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH,
    source_2424_research_doc_path: Annotated[
        Path, typer.Option("--source-2424-research-doc")
    ] = m2427.DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH,
    source_2424_route_doc_path: Annotated[
        Path, typer.Option("--source-2424-route-doc")
    ] = m2427.DEFAULT_SOURCE_2424_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2427.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2427.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2427.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2427.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2427.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2427.run_growth_tilt_engine_manual_review_packet_dry_run(
        source_2426_schedule_dry_run_result_path=(
            source_2426_schedule_dry_run_result_path
        ),
        source_2426_schedule_boundary_checklist_path=(
            source_2426_schedule_boundary_checklist_path
        ),
        source_2426_schedule_no_effect_audit_summary_path=(
            source_2426_schedule_no_effect_audit_summary_path
        ),
        source_2426_research_doc_path=source_2426_research_doc_path,
        source_2426_boundary_doc_path=source_2426_boundary_doc_path,
        source_2426_no_effect_doc_path=source_2426_no_effect_doc_path,
        source_2426_route_doc_path=source_2426_route_doc_path,
        source_2425_dry_run_wiring_result_path=(
            source_2425_dry_run_wiring_result_path
        ),
        source_2425_manual_review_handoff_wiring_plan_path=(
            source_2425_manual_review_handoff_wiring_plan_path
        ),
        source_2425_research_doc_path=source_2425_research_doc_path,
        source_2425_manual_review_doc_path=source_2425_manual_review_doc_path,
        source_2425_route_doc_path=source_2425_route_doc_path,
        source_2424_enablement_plan_result_path=(
            source_2424_enablement_plan_result_path
        ),
        source_2424_research_doc_path=source_2424_research_doc_path,
        source_2424_route_doc_path=source_2424_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine manual review packet dry-run",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "paper_shadow_schedule_dry_run_status",
        "paper_shadow_schedule_dry_run_ready",
        "schedule_dry_run_gap_count",
        "paper_shadow_dry_run_wiring_status",
        "paper_shadow_dry_run_wiring_ready",
        "enablement_plan_status",
        "enablement_plan_ready",
        "manual_review_packet_dry_run_started",
        "manual_review_packet_dry_run_completed",
        "manual_review_packet_dry_run_ready",
        "manual_review_packet_ready",
        "manual_review_checklist_ready",
        "no_advice_boundary_ready",
        "reviewer_handoff_manifest_ready",
        "manual_review_packet_gap_count",
        "missing_manual_review_evidence_count",
        "safety_boundary_gap_count",
        "packet_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_observe_only_signal_artifact_boundary_command(
    source_2427_manual_review_packet_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-packet-dry-run-result")
    ] = m2428.DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH,
    source_2427_manual_review_packet_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-packet")
    ] = m2428.DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_PATH,
    source_2427_manual_review_checklist_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-checklist")
    ] = m2428.DEFAULT_SOURCE_2427_MANUAL_REVIEW_CHECKLIST_PATH,
    source_2427_no_advice_boundary_summary_path: Annotated[
        Path, typer.Option("--source-2427-no-advice-boundary-summary")
    ] = m2428.DEFAULT_SOURCE_2427_NO_ADVICE_BOUNDARY_SUMMARY_PATH,
    source_2427_reviewer_handoff_manifest_path: Annotated[
        Path, typer.Option("--source-2427-reviewer-handoff-manifest")
    ] = m2428.DEFAULT_SOURCE_2427_REVIEWER_HANDOFF_MANIFEST_PATH,
    source_2427_research_doc_path: Annotated[
        Path, typer.Option("--source-2427-research-doc")
    ] = m2428.DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH,
    source_2427_packet_doc_path: Annotated[
        Path, typer.Option("--source-2427-packet-doc")
    ] = m2428.DEFAULT_SOURCE_2427_PACKET_DOC_PATH,
    source_2427_checklist_doc_path: Annotated[
        Path, typer.Option("--source-2427-checklist-doc")
    ] = m2428.DEFAULT_SOURCE_2427_CHECKLIST_DOC_PATH,
    source_2427_no_advice_doc_path: Annotated[
        Path, typer.Option("--source-2427-no-advice-doc")
    ] = m2428.DEFAULT_SOURCE_2427_NO_ADVICE_DOC_PATH,
    source_2427_handoff_doc_path: Annotated[
        Path, typer.Option("--source-2427-handoff-doc")
    ] = m2428.DEFAULT_SOURCE_2427_HANDOFF_DOC_PATH,
    source_2427_route_doc_path: Annotated[
        Path, typer.Option("--source-2427-route-doc")
    ] = m2428.DEFAULT_SOURCE_2427_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2428.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2428.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2428.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2428.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2428.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2428.run_growth_tilt_engine_observe_only_signal_artifact_boundary(
        source_2427_manual_review_packet_dry_run_result_path=(
            source_2427_manual_review_packet_dry_run_result_path
        ),
        source_2427_manual_review_packet_path=(
            source_2427_manual_review_packet_path
        ),
        source_2427_manual_review_checklist_path=(
            source_2427_manual_review_checklist_path
        ),
        source_2427_no_advice_boundary_summary_path=(
            source_2427_no_advice_boundary_summary_path
        ),
        source_2427_reviewer_handoff_manifest_path=(
            source_2427_reviewer_handoff_manifest_path
        ),
        source_2427_research_doc_path=source_2427_research_doc_path,
        source_2427_packet_doc_path=source_2427_packet_doc_path,
        source_2427_checklist_doc_path=source_2427_checklist_doc_path,
        source_2427_no_advice_doc_path=source_2427_no_advice_doc_path,
        source_2427_handoff_doc_path=source_2427_handoff_doc_path,
        source_2427_route_doc_path=source_2427_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine observe-only signal artifact boundary",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "manual_review_packet_dry_run_status",
        "manual_review_packet_dry_run_ready",
        "manual_review_packet_gap_count",
        "manual_review_packet_ready",
        "manual_review_checklist_ready",
        "prior_no_advice_boundary_ready",
        "reviewer_handoff_manifest_ready",
        "observe_only_signal_artifact_boundary_started",
        "observe_only_signal_artifact_boundary_completed",
        "observe_only_signal_artifact_boundary_ready",
        "signal_artifact_schema_ready",
        "valid_until_required",
        "valid_until_requirements_ready",
        "source_traceability_required",
        "source_traceability_requirements_ready",
        "pit_contract_manual_review_requirements_ready",
        "no_trading_advice_boundary_ready",
        "observe_only_signal_artifact_boundary_gap_count",
        "missing_observe_only_boundary_evidence_count",
        "safety_boundary_gap_count",
        "signal_artifact_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "signal_artifact_instance_generated",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_forward_outcome_binding_boundary_command(
    source_2428_observe_only_boundary_result_path: Annotated[
        Path, typer.Option("--source-2428-observe-only-boundary-result")
    ] = m2429.DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH,
    source_2428_signal_artifact_schema_path: Annotated[
        Path, typer.Option("--source-2428-signal-artifact-schema")
    ] = m2429.DEFAULT_SOURCE_2428_SIGNAL_ARTIFACT_SCHEMA_PATH,
    source_2428_valid_until_requirements_path: Annotated[
        Path, typer.Option("--source-2428-valid-until-requirements")
    ] = m2429.DEFAULT_SOURCE_2428_VALID_UNTIL_REQUIREMENTS_PATH,
    source_2428_source_traceability_requirements_path: Annotated[
        Path, typer.Option("--source-2428-source-traceability-requirements")
    ] = m2429.DEFAULT_SOURCE_2428_SOURCE_TRACEABILITY_REQUIREMENTS_PATH,
    source_2428_pit_contract_manual_review_requirements_path: Annotated[
        Path, typer.Option("--source-2428-pit-contract-manual-review-requirements")
    ] = m2429.DEFAULT_SOURCE_2428_PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_PATH,
    source_2428_no_trading_advice_boundary_path: Annotated[
        Path, typer.Option("--source-2428-no-trading-advice-boundary")
    ] = m2429.DEFAULT_SOURCE_2428_NO_TRADING_ADVICE_BOUNDARY_PATH,
    source_2428_research_doc_path: Annotated[
        Path, typer.Option("--source-2428-research-doc")
    ] = m2429.DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH,
    source_2428_schema_doc_path: Annotated[
        Path, typer.Option("--source-2428-schema-doc")
    ] = m2429.DEFAULT_SOURCE_2428_SCHEMA_DOC_PATH,
    source_2428_valid_until_doc_path: Annotated[
        Path, typer.Option("--source-2428-valid-until-doc")
    ] = m2429.DEFAULT_SOURCE_2428_VALID_UNTIL_DOC_PATH,
    source_2428_traceability_doc_path: Annotated[
        Path, typer.Option("--source-2428-traceability-doc")
    ] = m2429.DEFAULT_SOURCE_2428_TRACEABILITY_DOC_PATH,
    source_2428_no_advice_doc_path: Annotated[
        Path, typer.Option("--source-2428-no-advice-doc")
    ] = m2429.DEFAULT_SOURCE_2428_NO_ADVICE_DOC_PATH,
    source_2428_route_doc_path: Annotated[
        Path, typer.Option("--source-2428-route-doc")
    ] = m2429.DEFAULT_SOURCE_2428_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2429.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2429.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2429.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2429.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2429.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2429.run_growth_tilt_engine_forward_outcome_binding_boundary(
        source_2428_observe_only_boundary_result_path=(
            source_2428_observe_only_boundary_result_path
        ),
        source_2428_signal_artifact_schema_path=(
            source_2428_signal_artifact_schema_path
        ),
        source_2428_valid_until_requirements_path=(
            source_2428_valid_until_requirements_path
        ),
        source_2428_source_traceability_requirements_path=(
            source_2428_source_traceability_requirements_path
        ),
        source_2428_pit_contract_manual_review_requirements_path=(
            source_2428_pit_contract_manual_review_requirements_path
        ),
        source_2428_no_trading_advice_boundary_path=(
            source_2428_no_trading_advice_boundary_path
        ),
        source_2428_research_doc_path=source_2428_research_doc_path,
        source_2428_schema_doc_path=source_2428_schema_doc_path,
        source_2428_valid_until_doc_path=source_2428_valid_until_doc_path,
        source_2428_traceability_doc_path=source_2428_traceability_doc_path,
        source_2428_no_advice_doc_path=source_2428_no_advice_doc_path,
        source_2428_route_doc_path=source_2428_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine forward outcome binding boundary",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "observe_only_signal_artifact_boundary_status",
        "observe_only_signal_artifact_boundary_ready",
        "prior_signal_artifact_schema_ready",
        "prior_valid_until_requirements_ready",
        "prior_source_traceability_requirements_ready",
        "prior_pit_contract_manual_review_requirements_ready",
        "prior_no_trading_advice_boundary_ready",
        "forward_outcome_binding_boundary_started",
        "forward_outcome_binding_boundary_completed",
        "forward_outcome_binding_boundary_ready",
        "outcome_horizons",
        "outcome_horizon_rules_ready",
        "outcome_schema_ready",
        "valid_until_binding_ready",
        "outcome_decision_rules_ready",
        "baseline_comparison_ready",
        "signal_to_outcome_linkage_ready",
        "no_effect_boundary_ready",
        "forward_outcome_binding_boundary_gap_count",
        "missing_binding_boundary_evidence_count",
        "safety_boundary_gap_count",
        "outcome_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_candidate_promotion_evidence_review_command(
    source_2426_schedule_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2426-schedule-dry-run-result")
    ] = m2430.DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH,
    source_2427_manual_review_packet_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-packet-dry-run-result")
    ] = m2430.DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH,
    source_2428_observe_only_boundary_result_path: Annotated[
        Path, typer.Option("--source-2428-observe-only-boundary-result")
    ] = m2430.DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH,
    source_2429_forward_outcome_boundary_result_path: Annotated[
        Path, typer.Option("--source-2429-forward-outcome-boundary-result")
    ] = m2430.DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_RESULT_PATH,
    candidate_registry_path: Annotated[
        Path, typer.Option("--candidate-registry")
    ] = m2430.DEFAULT_CANDIDATE_REGISTRY_PATH,
    prior_candidate_evidence_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence")
    ] = m2430.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH,
    source_2426_research_doc_path: Annotated[
        Path, typer.Option("--source-2426-research-doc")
    ] = m2430.DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH,
    source_2427_research_doc_path: Annotated[
        Path, typer.Option("--source-2427-research-doc")
    ] = m2430.DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH,
    source_2428_research_doc_path: Annotated[
        Path, typer.Option("--source-2428-research-doc")
    ] = m2430.DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH,
    source_2429_research_doc_path: Annotated[
        Path, typer.Option("--source-2429-research-doc")
    ] = m2430.DEFAULT_SOURCE_2429_RESEARCH_DOC_PATH,
    source_2429_route_doc_path: Annotated[
        Path, typer.Option("--source-2429-route-doc")
    ] = m2430.DEFAULT_SOURCE_2429_ROUTE_DOC_PATH,
    prior_candidate_evidence_doc_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence-doc")
    ] = m2430.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2430.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2430.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2430.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2430.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2430.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2430.run_growth_tilt_engine_candidate_promotion_evidence_review(
        source_2426_schedule_dry_run_result_path=(
            source_2426_schedule_dry_run_result_path
        ),
        source_2427_manual_review_packet_dry_run_result_path=(
            source_2427_manual_review_packet_dry_run_result_path
        ),
        source_2428_observe_only_boundary_result_path=(
            source_2428_observe_only_boundary_result_path
        ),
        source_2429_forward_outcome_boundary_result_path=(
            source_2429_forward_outcome_boundary_result_path
        ),
        candidate_registry_path=candidate_registry_path,
        prior_candidate_evidence_path=prior_candidate_evidence_path,
        source_2426_research_doc_path=source_2426_research_doc_path,
        source_2427_research_doc_path=source_2427_research_doc_path,
        source_2428_research_doc_path=source_2428_research_doc_path,
        source_2429_research_doc_path=source_2429_research_doc_path,
        source_2429_route_doc_path=source_2429_route_doc_path,
        prior_candidate_evidence_doc_path=prior_candidate_evidence_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine candidate promotion evidence review",
        payload,
    )
    for field in (
        "readiness_status",
        "schedule_dry_run_ready",
        "manual_review_packet_dry_run_ready",
        "observe_only_signal_artifact_boundary_ready",
        "forward_outcome_binding_boundary_ready",
        "candidate_registry_ready",
        "prior_candidate_evidence_ready",
        "promotion_evidence_review_started",
        "promotion_evidence_review_completed",
        "promotion_evidence_review_ready",
        "promotion_candidate_found",
        "promotion_candidate_count",
        "candidate_count",
        "candidate_evidence_matrix_ready",
        "candidate_decision_summary_ready",
        "no_promotion_rationale_ready",
        "engineering_readiness_is_alpha_evidence",
        "paper_shadow_promotion_allowed_by_registry",
        "prior_owner_approved_paper_shadow",
        "prior_owner_approved_observation",
        "promotion_evidence_review_gap_count",
        "missing_promotion_review_evidence_count",
        "safety_boundary_gap_count",
        "candidate_evidence_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_existing_candidate_evidence_matrix_command(
    source_2430_promotion_review_result_path: Annotated[
        Path, typer.Option("--source-2430-promotion-review-result")
    ] = m2431.DEFAULT_SOURCE_2430_PROMOTION_REVIEW_RESULT_PATH,
    candidate_registry_path: Annotated[
        Path, typer.Option("--candidate-registry")
    ] = m2431.DEFAULT_CANDIDATE_REGISTRY_PATH,
    prior_candidate_evidence_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence")
    ] = m2431.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH,
    prior_component_value_matrix_path: Annotated[
        Path, typer.Option("--prior-component-value-matrix")
    ] = m2431.DEFAULT_PRIOR_COMPONENT_VALUE_MATRIX_PATH,
    component_value_doc_path: Annotated[
        Path, typer.Option("--component-value-doc")
    ] = m2431.DEFAULT_COMPONENT_VALUE_DOC_PATH,
    prior_candidate_evidence_doc_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence-doc")
    ] = m2431.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH,
    candidate_reclassification_doc_path: Annotated[
        Path, typer.Option("--candidate-reclassification-doc")
    ] = m2431.DEFAULT_CANDIDATE_RECLASSIFICATION_DOC_PATH,
    execution_semantics_review_doc_path: Annotated[
        Path, typer.Option("--execution-semantics-review-doc")
    ] = m2431.DEFAULT_EXECUTION_SEMANTICS_REVIEW_DOC_PATH,
    growth_tilt_signal_doc_path: Annotated[
        Path, typer.Option("--growth-tilt-signal-doc")
    ] = m2431.DEFAULT_GROWTH_TILT_SIGNAL_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2431.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2431.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2431.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2431.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2431.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2431.run_growth_tilt_existing_candidate_evidence_matrix(
        source_2430_promotion_review_result_path=(
            source_2430_promotion_review_result_path
        ),
        candidate_registry_path=candidate_registry_path,
        prior_candidate_evidence_path=prior_candidate_evidence_path,
        prior_component_value_matrix_path=prior_component_value_matrix_path,
        component_value_doc_path=component_value_doc_path,
        prior_candidate_evidence_doc_path=prior_candidate_evidence_doc_path,
        candidate_reclassification_doc_path=candidate_reclassification_doc_path,
        execution_semantics_review_doc_path=execution_semantics_review_doc_path,
        growth_tilt_signal_doc_path=growth_tilt_signal_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt existing candidate evidence matrix",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2430_ready",
        "candidate_registry_ready",
        "prior_candidate_evidence_ready",
        "component_value_evidence_ready",
        "existing_candidate_evidence_matrix_ready",
        "candidate_status_summary_ready",
        "candidate_metric_coverage_ready",
        "no_effect_boundary_ready",
        "candidate_count",
        "required_candidate_group_count",
        "rejected_count",
        "component_value_count",
        "needs_pit_count",
        "promotion_candidate_count",
        "promotion_candidate_found",
        "metric_coverage_available_count",
        "metric_coverage_partial_count",
        "metric_coverage_missing_count",
        "evidence_gap_count",
        "engineering_readiness_is_alpha_evidence",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "candidate_gauntlet_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_candidate_gauntlet_command(
    source_2431_existing_candidate_evidence_matrix_path: Annotated[
        Path, typer.Option("--source-2431-existing-candidate-evidence-matrix")
    ] = m2432.DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_MATRIX_PATH,
    candidate_set_path: Annotated[
        Path, typer.Option("--candidate-set")
    ] = m2432.DEFAULT_CANDIDATE_SET_PATH,
    existing_candidate_evidence_matrix_doc_path: Annotated[
        Path, typer.Option("--existing-candidate-evidence-matrix-doc")
    ] = m2432.DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_DOC_PATH,
    existing_candidate_evidence_matrix_table_doc_path: Annotated[
        Path, typer.Option("--existing-candidate-evidence-matrix-table-doc")
    ] = m2432.DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_TABLE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2432.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2432.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2432.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2432.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2432.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2432.run_growth_tilt_candidate_gauntlet_harness(
        source_2431_existing_candidate_evidence_matrix_path=(
            source_2431_existing_candidate_evidence_matrix_path
        ),
        candidate_set_path=candidate_set_path,
        existing_candidate_evidence_matrix_doc_path=(
            existing_candidate_evidence_matrix_doc_path
        ),
        existing_candidate_evidence_matrix_table_doc_path=(
            existing_candidate_evidence_matrix_table_doc_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt candidate gauntlet harness",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2431_ready",
        "candidate_set_ready",
        "candidate_set_id",
        "harness_ready",
        "baseline_ready",
        "metrics_ready",
        "kill_criteria_ready",
        "promotion_criteria_ready",
        "regime_slices_ready",
        "parameter_plateau_check_ready",
        "ablation_output_ready",
        "candidate_group_count",
        "candidates_tested",
        "required_metric_count",
        "configured_metric_count",
        "kill_criteria_count",
        "promotion_criteria_count",
        "regime_slice_count",
        "parameter_plateau_dimension_count",
        "ablation_output_count",
        "new_investment_threshold_values_set",
        "threshold_policy_required_for_execution",
        "criteria_threshold_values_all_null",
        "contract_gap_count",
        "candidate_gauntlet_run",
        "candidate_batch_screen_run",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_false_risk_off_missed_upside_batch_screen_command(
    source_2432_candidate_gauntlet_harness_path: Annotated[
        Path, typer.Option("--source-2432-candidate-gauntlet-harness")
    ] = m2433.DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_HARNESS_PATH,
    candidate_set_path: Annotated[
        Path, typer.Option("--candidate-set")
    ] = m2433.DEFAULT_CANDIDATE_SET_PATH,
    candidate_gauntlet_harness_doc_path: Annotated[
        Path, typer.Option("--candidate-gauntlet-harness-doc")
    ] = m2433.DEFAULT_CANDIDATE_GAUNTLET_HARNESS_DOC_PATH,
    candidate_set_2432_doc_path: Annotated[
        Path, typer.Option("--candidate-set-2432-doc")
    ] = m2433.DEFAULT_CANDIDATE_SET_2432_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2433.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2433.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2433.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2433.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2433.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2433.run_growth_tilt_false_risk_off_missed_upside_batch_screen(
        source_2432_candidate_gauntlet_harness_path=(
            source_2432_candidate_gauntlet_harness_path
        ),
        candidate_set_path=candidate_set_path,
        candidate_gauntlet_harness_doc_path=candidate_gauntlet_harness_doc_path,
        candidate_set_2432_doc_path=candidate_set_2432_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt false risk-off missed upside batch screen",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2432_ready",
        "candidate_set_ready",
        "candidate_set_id",
        "batch_screen_ready",
        "candidate_screen_matrix_ready",
        "batch_decision_summary_ready",
        "research_question_coverage_ready",
        "no_effect_boundary_ready",
        "candidate_count",
        "candidates_screened",
        "rejected_count",
        "component_value_count",
        "pit_candidate_count",
        "promotion_candidate_count",
        "promotion_candidate_found",
        "research_question_count",
        "research_question_covered_count",
        "new_investment_threshold_values_set",
        "threshold_policy_required_for_pit_or_promotion",
        "criteria_threshold_values_all_null",
        "computed_new_metrics",
        "screen_contract_gap_count",
        "candidate_batch_screen_run",
        "market_data_candidate_screen_run",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_defensive_limited_adjustment_component_validation_command(
    source_2433_batch_screen_path: Annotated[
        Path, typer.Option("--source-2433-batch-screen")
    ] = m2434.DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH,
    batch_screen_doc_path: Annotated[
        Path, typer.Option("--batch-screen-doc")
    ] = m2434.DEFAULT_BATCH_SCREEN_DOC_PATH,
    candidate_screen_matrix_doc_path: Annotated[
        Path, typer.Option("--candidate-screen-matrix-doc")
    ] = m2434.DEFAULT_CANDIDATE_SCREEN_MATRIX_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2434.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2434.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2434.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2434.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2434.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2434.run_growth_tilt_defensive_limited_adjustment_component_validation(
            source_2433_batch_screen_path=source_2433_batch_screen_path,
            batch_screen_doc_path=batch_screen_doc_path,
            candidate_screen_matrix_doc_path=candidate_screen_matrix_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt defensive limited adjustment component validation",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2433_ready",
        "source_candidate_found",
        "component_validation_ready",
        "component_value_assessment_ready",
        "primary_value_matrix_ready",
        "validation_boundary_ready",
        "component_value_found",
        "candidate_status",
        "promotion_candidate_found",
        "promotion_candidate_count",
        "computed_new_metrics",
        "market_data_component_validation_run",
        "evidence_gap_count",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    primary_value = payload.get("primary_value")
    if not isinstance(primary_value, list):
        primary_value = []
    console.print(f"primary_value={','.join(str(value) for value in primary_value)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_valid_until_outcome_hit_rate_study_command(
    source_2434_component_validation_path: Annotated[
        Path, typer.Option("--source-2434-component-validation")
    ] = m2435.DEFAULT_SOURCE_2434_COMPONENT_VALIDATION_PATH,
    source_2418_valid_until_alignment_path: Annotated[
        Path, typer.Option("--source-2418-valid-until-alignment")
    ] = m2435.DEFAULT_SOURCE_2418_VALID_UNTIL_ALIGNMENT_PATH,
    source_2418_stale_signal_policy_path: Annotated[
        Path, typer.Option("--source-2418-stale-signal-policy")
    ] = m2435.DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_PATH,
    source_2429_forward_outcome_boundary_path: Annotated[
        Path, typer.Option("--source-2429-forward-outcome-boundary")
    ] = m2435.DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_PATH,
    candidate_set_2432_path: Annotated[
        Path, typer.Option("--candidate-set-2432")
    ] = m2435.DEFAULT_CANDIDATE_SET_2432_PATH,
    component_validation_doc_path: Annotated[
        Path, typer.Option("--component-validation-doc")
    ] = m2435.DEFAULT_COMPONENT_VALIDATION_DOC_PATH,
    valid_until_alignment_doc_path: Annotated[
        Path, typer.Option("--valid-until-alignment-doc")
    ] = m2435.DEFAULT_VALID_UNTIL_ALIGNMENT_DOC_PATH,
    forward_outcome_boundary_doc_path: Annotated[
        Path, typer.Option("--forward-outcome-boundary-doc")
    ] = m2435.DEFAULT_FORWARD_OUTCOME_BOUNDARY_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2435.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2435.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2435.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2435.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2435.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2435.run_growth_tilt_valid_until_outcome_hit_rate_study(
        source_2434_component_validation_path=source_2434_component_validation_path,
        source_2418_valid_until_alignment_path=source_2418_valid_until_alignment_path,
        source_2418_stale_signal_policy_path=source_2418_stale_signal_policy_path,
        source_2429_forward_outcome_boundary_path=(
            source_2429_forward_outcome_boundary_path
        ),
        candidate_set_2432_path=candidate_set_2432_path,
        component_validation_doc_path=component_validation_doc_path,
        valid_until_alignment_doc_path=valid_until_alignment_doc_path,
        forward_outcome_boundary_doc_path=forward_outcome_boundary_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt valid-until outcome hit-rate study",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2434_ready",
        "source_2418_valid_until_evidence_ready",
        "source_2429_forward_outcome_boundary_ready",
        "candidate_set_valid_until_metric_ready",
        "candidate_set_valid_until_candidate_group_ready",
        "hit_rate_study_ready",
        "valid_until_hit_rate_matrix_ready",
        "stale_signal_reduction_summary_ready",
        "expiry_failure_audit_ready",
        "no_effect_boundary_ready",
        "valid_until_component_value_found",
        "valid_until_hit_rate_delta",
        "stale_signal_reduction",
        "expiry_failure_count",
        "candidate_status",
        "outcome_sample_count",
        "observed_outcome_hit_rate_available",
        "computed_new_metrics",
        "market_data_hit_rate_study_run",
        "real_outcome_binding_run",
        "evidence_gap_count",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _cli_scalar(value: object) -> object:
    if isinstance(value, bool):
        return str(value).lower()
    return value


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
