from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "weight_search_space_v2.yaml"
)
DEFAULT_WEIGHT_SEARCH_SPACE_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_search_space"
DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_experiment_batch2"
)
DEFAULT_WEIGHT_BATCH_BACKFILL_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_batch_backfill"
DEFAULT_WEIGHT_SCORECARD_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_scorecard"
DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_robustness_review"
)
DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_adaptive_branch"
DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_expanded_search"
DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_candidate_cluster"
)
DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_top_candidate_interpretation"
)
DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_method_promotion_gate"
)
DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "formal_method_auto_plan"
)
DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weight_search_dashboard"
)
DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_research_decision_pack"
)
DEFAULT_NO_PROMOTION_REVIEW_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "no_promotion_review"
DEFAULT_NEAR_MISS_CANDIDATES_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "near_miss_candidates"
DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "cash_buffer_attribution"
)
DEFAULT_SEARCH_COVERAGE_GAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "search_coverage_gap"
DEFAULT_TARGETED_SEARCH_V3_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "targeted_search_v3"
DEFAULT_TARGETED_V3_BACKFILL_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "targeted_v3_backfill"
DEFAULT_NEAR_MISS_AB_COMPARISON_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "near_miss_ab_comparison"
)
DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "promotion_threshold_sensitivity"
)
DEFAULT_CANDIDATE_PROMOTION_V2_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_promotion_v2"
DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "next_formal_or_search_plan"
)
DEFAULT_GATE_CALIBRATION_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "gate_calibration_review"
)
DEFAULT_SCORECARD_ATTRIBUTION_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "scorecard_attribution"
DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_instability_diagnosis"
)
DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "consensus_quality_review"
)
DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "micro_search_v4_design"
DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "micro_search_v4_backfill"
)
DEFAULT_GATE_CALIBRATED_REVIEW_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "gate_calibrated_review"
DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_vs_parameter_attribution"
)
DEFAULT_NEXT_RESEARCH_DIRECTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "next_research_direction"
)
DEFAULT_OWNER_RESEARCH_ROADMAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_research_roadmap"
DEFAULT_SIGNAL_FAILURE_TAXONOMY_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "signal_feature_failure_taxonomy_v1.yaml"
)
DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_failure_taxonomy"
)
DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_signal_ledger"
)
DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_churn_root_cause"
)
DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "regime_mismatch_attribution"
)
DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_quality_filter_design"
)
DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_backfill"
)
DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_vs_original_comparison"
)
DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_gate_experiment"
DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_promotion_review"
)
DEFAULT_OWNER_SIGNAL_ROADMAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_signal_roadmap"

SEARCH_REQUIRED_FAMILIES = (
    "smoothing",
    "cooldown",
    "regime_gating",
    "rebalance_threshold",
    "candidate_ensemble",
    "cash_buffer",
    "risk_exposure_control",
    "turnover_control",
)

# TRADING-286_to_305 pilot screening policy. These constants only rank
# research-screening variants and are documented in the requirement file; they
# do not approve production weights or size positions.
BATCH2_PROMOTE_SCORE = 0.72
BATCH2_KEEP_TESTING_SCORE = 0.56
BATCH2_MATERIAL_DRAWDOWN_WORSE_DELTA = -0.002
BATCH2_MATERIAL_TURNOVER_WORSE_DELTA = 0.25
BATCH2_STRONG_RECOVERY_HIGH_LAG_DELTA = -0.02
BATCH2_SIDWAYS_WORSE_REGIME_LABEL = "WORSE"

BATCH2_SCORE_WEIGHTS: dict[str, float] = {
    "return": 0.16,
    "annualized_return": 0.08,
    "drawdown": 0.14,
    "volatility": 0.07,
    "risk_adjusted_return": 0.10,
    "turnover": 0.10,
    "rolling_consistency": 0.10,
    "sideways_choppy": 0.06,
    "tech_drawdown": 0.05,
    "strong_recovery_lag": 0.05,
    "signal_churn": 0.04,
    "weight_jumps": 0.03,
    "simplicity": 0.01,
    "data_quality": 0.01,
}

# TRADING-306_to_315 diagnostic pilot bands. They classify research-only
# near-miss and sensitivity evidence and are documented in the requirement file;
# they do not relax the production gate or approve target weights.
NEAR_MISS_MIN_OVERALL_SCORE = 0.48
NEAR_MISS_MIN_COMPONENT_SCORE = 0.65
NEAR_MISS_MAX_FAILED_GATES = 2
NO_PROMOTION_NEAR_MISS_MARGIN = 0.06
TARGETED_V3_MAX_VARIANTS = 120

# TRADING-316_to_325 diagnostic pilot constants. They are documented in the
# requirement file and only shape research-only diagnosis / micro-search review;
# they do not change official promotion policy or production target weights.
GATE_DIAGNOSTIC_RELAXATION = 0.05
V4_MICRO_MIN_VARIANTS = 20
V4_MICRO_MAX_VARIANTS = 40
SIGNAL_INSTABILITY_LARGE_JUMP_REVIEW_COUNT = 2
SIGNAL_INSTABILITY_CHURN_REVIEW_COUNT = 4
CONSENSUS_HIGH_DISPERSION = 0.15
CONSENSUS_MODERATE_DISPERSION = 0.08

# TRADING-326_to_335 signal-quality pilot constants. They are documented in the
# requirement file and only classify research-only signal events / filtered
# candidate prototypes; they do not approve target weights or broker actions.
SIGNAL_QUALITY_PERSISTENCE_DAYS = 3
SIGNAL_QUALITY_HIGH_FLIP_COUNT = 4
SIGNAL_QUALITY_HARMFUL_EVENT_SHARE = 0.35
CANDIDATE_LEDGER_METHODS = (
    "limited_adjustment",
    "smooth_weights_3d_limited_adjustment",
    "smooth_weights_5d_limited_adjustment",
    "median_target_weights",
    "top5_candidate_consensus",
    "cash_buffer_10_plus_smooth_2d_alpha_40",
)

PROMOTION_GATE_UNIVERSE = (
    "composite_score_gate",
    "return_preservation_gate",
    "drawdown_gate",
    "rolling_consistency_gate",
    "turnover_gate",
    "regime_gate",
    "recovery_lag_gate",
    "data_quality_gate",
)

HARD_REJECT_GATE_MAP = {
    "data_quality_FAIL": "data_quality_gate",
    "max_drawdown_materially_worse_than_limited_adjustment": "drawdown_gate",
    "rolling_consistency_worse_than_limited_adjustment": "rolling_consistency_gate",
    "turnover_materially_higher_than_limited_adjustment": "turnover_gate",
    "strong_recovery_lag_cost_HIGH": "recovery_lag_gate",
    "sideways_choppy_performance_WORSE": "regime_gate",
    "only_wins_in_one_narrow_window_or_pressure_regimes_worse": "regime_gate",
}


def _call_weight_search_foundation(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation

    return getattr(dynamic_v3_weight_search_foundation, name)(*args, **kwargs)


def load_weight_search_space_config(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("load_weight_search_space_config", *args, **kwargs)


def validate_weight_search_space_config(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("validate_weight_search_space_config", *args, **kwargs)


def run_weight_search_space_validation(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("run_weight_search_space_validation", *args, **kwargs)


def weight_search_space_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("weight_search_space_report_payload", *args, **kwargs)


def validate_weight_search_space_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("validate_weight_search_space_artifact", *args, **kwargs)


def build_weight_experiment_batch2(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("build_weight_experiment_batch2", *args, **kwargs)


def weight_experiment_batch2_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation(
        "weight_experiment_batch2_report_payload", *args, **kwargs
    )


def validate_weight_experiment_batch2_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation(
        "validate_weight_experiment_batch2_artifact", *args, **kwargs
    )


def run_weight_batch_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("run_weight_batch_backfill", *args, **kwargs)


def resume_weight_batch_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("resume_weight_batch_backfill", *args, **kwargs)


def weight_batch_backfill_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation("weight_batch_backfill_report_payload", *args, **kwargs)


def validate_weight_batch_backfill_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_foundation(
        "validate_weight_batch_backfill_artifact", *args, **kwargs
    )


def _call_weight_search_evaluation(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import (
        dynamic_v3_weight_search_evaluation as evaluation,
    )

    return getattr(evaluation, name)(*args, **kwargs)


def run_weight_scorecard(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("run_weight_scorecard", *args, **kwargs)


def weight_scorecard_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("weight_scorecard_report_payload", *args, **kwargs)


def validate_weight_scorecard_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("validate_weight_scorecard_artifact", *args, **kwargs)


def run_weight_robustness_review(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("run_weight_robustness_review", *args, **kwargs)


def weight_robustness_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation(
        "weight_robustness_review_report_payload", *args, **kwargs
    )


def validate_weight_robustness_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation(
        "validate_weight_robustness_review_artifact", *args, **kwargs
    )


def run_weight_adaptive_branch(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("run_weight_adaptive_branch", *args, **kwargs)


def weight_adaptive_branch_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("weight_adaptive_branch_report_payload", *args, **kwargs)


def validate_weight_adaptive_branch_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation(
        "validate_weight_adaptive_branch_artifact", *args, **kwargs
    )


def build_weight_expanded_search(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("build_weight_expanded_search", *args, **kwargs)


def run_weight_expanded_search(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_evaluation("run_weight_expanded_search", *args, **kwargs)


def _call_weight_search_decision(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_decision as decision

    return getattr(decision, name)(*args, **kwargs)


def run_weight_candidate_cluster(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("run_weight_candidate_cluster", *args, **kwargs)


def weight_candidate_cluster_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("weight_candidate_cluster_report_payload", *args, **kwargs)


def validate_weight_candidate_cluster_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "validate_weight_candidate_cluster_artifact", *args, **kwargs
    )


def run_weight_top_candidate_interpretation(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("run_weight_top_candidate_interpretation", *args, **kwargs)


def weight_top_candidate_interpretation_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "weight_top_candidate_interpretation_report_payload", *args, **kwargs
    )


def validate_weight_top_candidate_interpretation_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "validate_weight_top_candidate_interpretation_artifact", *args, **kwargs
    )


def run_weight_method_promotion_gate(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("run_weight_method_promotion_gate", *args, **kwargs)


def weight_method_promotion_gate_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "weight_method_promotion_gate_report_payload", *args, **kwargs
    )


def validate_weight_method_promotion_gate_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "validate_weight_method_promotion_gate_artifact", *args, **kwargs
    )


def run_formal_method_auto_plan(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("run_formal_method_auto_plan", *args, **kwargs)


def formal_method_auto_plan_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("formal_method_auto_plan_report_payload", *args, **kwargs)


def validate_formal_method_auto_plan_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "validate_formal_method_auto_plan_artifact", *args, **kwargs
    )


def build_weight_search_dashboard(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("build_weight_search_dashboard", *args, **kwargs)


def weight_search_dashboard_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("weight_search_dashboard_report_payload", *args, **kwargs)


def validate_weight_search_dashboard_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "validate_weight_search_dashboard_artifact", *args, **kwargs
    )


def build_owner_research_decision_pack(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision("build_owner_research_decision_pack", *args, **kwargs)


def owner_research_decision_pack_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "owner_research_decision_pack_report_payload", *args, **kwargs
    )


def validate_owner_research_decision_pack_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_decision(
        "validate_owner_research_decision_pack_artifact", *args, **kwargs
    )


def _call_weight_search_diagnostics(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics

    return getattr(dynamic_v3_weight_search_diagnostics, name)(*args, **kwargs)


def run_no_promotion_review(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("run_no_promotion_review", *args, **kwargs)


def no_promotion_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("no_promotion_review_report_payload", *args, **kwargs)


def validate_no_promotion_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("validate_no_promotion_review_artifact", *args, **kwargs)


def extract_near_miss_candidates(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("extract_near_miss_candidates", *args, **kwargs)


def near_miss_candidates_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("near_miss_candidates_report_payload", *args, **kwargs)


def validate_near_miss_candidates_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics(
        "validate_near_miss_candidates_artifact", *args, **kwargs
    )


def run_cash_buffer_attribution(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("run_cash_buffer_attribution", *args, **kwargs)


def cash_buffer_attribution_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics(
        "cash_buffer_attribution_report_payload", *args, **kwargs
    )


def validate_cash_buffer_attribution_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics(
        "validate_cash_buffer_attribution_artifact", *args, **kwargs
    )


def run_search_coverage_gap(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("run_search_coverage_gap", *args, **kwargs)


def search_coverage_gap_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("search_coverage_gap_report_payload", *args, **kwargs)


def validate_search_coverage_gap_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_diagnostics("validate_search_coverage_gap_artifact", *args, **kwargs)


def _call_weight_search_targeted(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted

    return getattr(dynamic_v3_weight_search_targeted, name)(*args, **kwargs)


def build_targeted_search_v3(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("build_targeted_search_v3", *args, **kwargs)


def targeted_search_v3_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("targeted_search_v3_report_payload", *args, **kwargs)


def validate_targeted_search_v3_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("validate_targeted_search_v3_artifact", *args, **kwargs)


def run_targeted_v3_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("run_targeted_v3_backfill", *args, **kwargs)


def resume_targeted_v3_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("resume_targeted_v3_backfill", *args, **kwargs)


def targeted_v3_backfill_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("targeted_v3_backfill_report_payload", *args, **kwargs)


def validate_targeted_v3_backfill_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("validate_targeted_v3_backfill_artifact", *args, **kwargs)


def run_near_miss_ab_comparison(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("run_near_miss_ab_comparison", *args, **kwargs)


def near_miss_ab_comparison_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted("near_miss_ab_comparison_report_payload", *args, **kwargs)


def validate_near_miss_ab_comparison_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_targeted(
        "validate_near_miss_ab_comparison_artifact", *args, **kwargs
    )


def _call_weight_search_followup(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_followup

    return getattr(dynamic_v3_weight_search_followup, name)(*args, **kwargs)


def run_promotion_threshold_sensitivity(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup("run_promotion_threshold_sensitivity", *args, **kwargs)


def promotion_threshold_sensitivity_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup(
        "promotion_threshold_sensitivity_report_payload", *args, **kwargs
    )


def validate_promotion_threshold_sensitivity_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup(
        "validate_promotion_threshold_sensitivity_artifact", *args, **kwargs
    )


def run_candidate_promotion_v2(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup("run_candidate_promotion_v2", *args, **kwargs)


def candidate_promotion_v2_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup("candidate_promotion_v2_report_payload", *args, **kwargs)


def validate_candidate_promotion_v2_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup("validate_candidate_promotion_v2_artifact", *args, **kwargs)


def run_next_formal_or_search_plan(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup("run_next_formal_or_search_plan", *args, **kwargs)


def next_formal_or_search_plan_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup(
        "next_formal_or_search_plan_report_payload", *args, **kwargs
    )


def validate_next_formal_or_search_plan_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_weight_search_followup(
        "validate_next_formal_or_search_plan_artifact", *args, **kwargs
    )


def _call_signal_diagnosis_foundation(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation

    return getattr(dynamic_v3_signal_diagnosis_foundation, name)(*args, **kwargs)


def run_gate_calibration_review(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation("run_gate_calibration_review", *args, **kwargs)


def gate_calibration_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "gate_calibration_review_report_payload", *args, **kwargs
    )


def validate_gate_calibration_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "validate_gate_calibration_review_artifact", *args, **kwargs
    )


def run_scorecard_attribution(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation("run_scorecard_attribution", *args, **kwargs)


def scorecard_attribution_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "scorecard_attribution_report_payload", *args, **kwargs
    )


def validate_scorecard_attribution_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "validate_scorecard_attribution_artifact", *args, **kwargs
    )


def run_signal_instability_diagnosis(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation("run_signal_instability_diagnosis", *args, **kwargs)


def signal_instability_diagnosis_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "signal_instability_diagnosis_report_payload", *args, **kwargs
    )


def validate_signal_instability_diagnosis_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "validate_signal_instability_diagnosis_artifact", *args, **kwargs
    )


def run_consensus_quality_review(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation("run_consensus_quality_review", *args, **kwargs)


def consensus_quality_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "consensus_quality_review_report_payload", *args, **kwargs
    )


def validate_consensus_quality_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_diagnosis_foundation(
        "validate_consensus_quality_review_artifact", *args, **kwargs
    )


def _call_micro_search_foundation(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_micro_search_foundation

    return getattr(dynamic_v3_micro_search_foundation, name)(*args, **kwargs)


def run_micro_search_v4_design(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("run_micro_search_v4_design", *args, **kwargs)


def micro_search_v4_design_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("micro_search_v4_design_report_payload", *args, **kwargs)


def validate_micro_search_v4_design_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation(
        "validate_micro_search_v4_design_artifact", *args, **kwargs
    )


def run_micro_search_v4_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("run_micro_search_v4_backfill", *args, **kwargs)


def micro_search_v4_backfill_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("micro_search_v4_backfill_report_payload", *args, **kwargs)


def validate_micro_search_v4_backfill_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation(
        "validate_micro_search_v4_backfill_artifact", *args, **kwargs
    )


def run_gate_calibrated_review(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("run_gate_calibrated_review", *args, **kwargs)


def gate_calibrated_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("gate_calibrated_review_report_payload", *args, **kwargs)


def validate_gate_calibrated_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation(
        "validate_gate_calibrated_review_artifact", *args, **kwargs
    )


def run_signal_vs_parameter_attribution(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation("run_signal_vs_parameter_attribution", *args, **kwargs)


def signal_vs_parameter_attribution_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation(
        "signal_vs_parameter_attribution_report_payload", *args, **kwargs
    )


def validate_signal_vs_parameter_attribution_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_micro_search_foundation(
        "validate_signal_vs_parameter_attribution_artifact", *args, **kwargs
    )


def _call_research_direction_foundation(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_research_direction_foundation

    return getattr(dynamic_v3_research_direction_foundation, name)(*args, **kwargs)


def run_next_research_direction(*args: Any, **kwargs: Any) -> Any:
    return _call_research_direction_foundation("run_next_research_direction", *args, **kwargs)


def next_research_direction_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_research_direction_foundation(
        "next_research_direction_report_payload", *args, **kwargs
    )


def validate_next_research_direction_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_research_direction_foundation(
        "validate_next_research_direction_artifact", *args, **kwargs
    )


def update_owner_research_roadmap(*args: Any, **kwargs: Any) -> Any:
    return _call_research_direction_foundation("update_owner_research_roadmap", *args, **kwargs)


def owner_research_roadmap_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_research_direction_foundation(
        "owner_research_roadmap_report_payload", *args, **kwargs
    )


def validate_owner_research_roadmap_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_research_direction_foundation(
        "validate_owner_research_roadmap_artifact", *args, **kwargs
    )


def _call_signal_filter_foundation(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_signal_filter_foundation

    return getattr(dynamic_v3_signal_filter_foundation, name)(*args, **kwargs)


def run_signal_failure_taxonomy_validation(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("run_signal_failure_taxonomy_validation", *args, **kwargs)


def signal_failure_taxonomy_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("signal_failure_taxonomy_report_payload", *args, **kwargs)


def validate_signal_failure_taxonomy_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "validate_signal_failure_taxonomy_artifact", *args, **kwargs
    )


def build_candidate_signal_ledger(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("build_candidate_signal_ledger", *args, **kwargs)


def candidate_signal_ledger_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("candidate_signal_ledger_report_payload", *args, **kwargs)


def validate_candidate_signal_ledger_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "validate_candidate_signal_ledger_artifact", *args, **kwargs
    )


def run_signal_churn_root_cause_review(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("run_signal_churn_root_cause_review", *args, **kwargs)


def signal_churn_root_cause_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("signal_churn_root_cause_report_payload", *args, **kwargs)


def validate_signal_churn_root_cause_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "validate_signal_churn_root_cause_artifact", *args, **kwargs
    )


def run_regime_mismatch_attribution(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("run_regime_mismatch_attribution", *args, **kwargs)


def regime_mismatch_attribution_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "regime_mismatch_attribution_report_payload", *args, **kwargs
    )


def validate_regime_mismatch_attribution_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "validate_regime_mismatch_attribution_artifact", *args, **kwargs
    )


def run_candidate_quality_filter_design(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation("run_candidate_quality_filter_design", *args, **kwargs)


def candidate_quality_filter_design_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "candidate_quality_filter_design_report_payload", *args, **kwargs
    )


def validate_candidate_quality_filter_design_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_signal_filter_foundation(
        "validate_candidate_quality_filter_design_artifact", *args, **kwargs
    )


def _call_filtered_candidate_pipeline(name: str, *args: Any, **kwargs: Any) -> Any:
    from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_pipeline

    return getattr(dynamic_v3_filtered_candidate_pipeline, name)(*args, **kwargs)


def run_filtered_candidate_backfill(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline("run_filtered_candidate_backfill", *args, **kwargs)


def filtered_candidate_backfill_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "filtered_candidate_backfill_report_payload", *args, **kwargs
    )


def validate_filtered_candidate_backfill_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "validate_filtered_candidate_backfill_artifact", *args, **kwargs
    )


def run_filtered_vs_original_comparison(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "run_filtered_vs_original_comparison", *args, **kwargs
    )


def filtered_vs_original_comparison_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "filtered_vs_original_comparison_report_payload", *args, **kwargs
    )


def validate_filtered_vs_original_comparison_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "validate_filtered_vs_original_comparison_artifact", *args, **kwargs
    )


def run_signal_gate_experiment(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline("run_signal_gate_experiment", *args, **kwargs)


def signal_gate_experiment_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "signal_gate_experiment_report_payload", *args, **kwargs
    )


def validate_signal_gate_experiment_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "validate_signal_gate_experiment_artifact", *args, **kwargs
    )


def run_filtered_candidate_promotion_review(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "run_filtered_candidate_promotion_review", *args, **kwargs
    )


def filtered_candidate_promotion_review_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "filtered_candidate_promotion_review_report_payload", *args, **kwargs
    )


def validate_filtered_candidate_promotion_review_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "validate_filtered_candidate_promotion_review_artifact", *args, **kwargs
    )


def build_owner_signal_roadmap(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline("build_owner_signal_roadmap", *args, **kwargs)


def owner_signal_roadmap_report_payload(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "owner_signal_roadmap_report_payload", *args, **kwargs
    )


def validate_owner_signal_roadmap_artifact(*args: Any, **kwargs: Any) -> Any:
    return _call_filtered_candidate_pipeline(
        "validate_owner_signal_roadmap_artifact", *args, **kwargs
    )
def render_micro_search_v4_design_report(
    manifest: Mapping[str, Any],
    rationale: Mapping[str, Any],
    variants: Sequence[Mapping[str, Any]],
) -> str:
    variant_lines = [
        f"- {row.get('variant_id')}: base={row.get('base_method')} "
        f"targets={','.join(_texts(row.get('target_failure_modes')))}"
        for row in variants
    ]
    return "\n".join(
        [
            f"# Micro Search v4 Design {manifest.get('v4_design_id')}",
            "",
            f"- variant_count：{manifest.get('variant_count')}",
            f"- recommended_focus：{', '.join(_texts(rationale.get('recommended_focus')))}",
            "",
            "## Variants",
            *variant_lines,
            "",
            "这些 v4 variants 只用于 micro search research screening，"
            "不是 official target weights。",
            "",
        ]
    )


def render_micro_search_v4_backfill_report(
    manifest: Mapping[str, Any],
    progress: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Micro Search v4 Backfill {manifest.get('v4_backfill_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- date range：{manifest.get('date_start')} -> {manifest.get('date_end')}",
            f"- data quality：{manifest.get('data_quality_status')}",
            "- variants completed："
            f"{progress.get('variants_completed')} / {progress.get('variants_total')}",
            "- safety：research screening only；no official target / no broker / no production",
            "",
        ]
    )


def render_gate_calibrated_review_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Gate-Calibrated Review {manifest.get('gate_review_id')}",
            "",
            f"- official_gate_promoted_count：{summary.get('official_gate_promoted_count')}",
            f"- diagnostic_gate_promoted_count：{summary.get('diagnostic_gate_promoted_count')}",
            f"- diagnostic_only_candidates："
            f"{', '.join(_texts(summary.get('diagnostic_only_candidates')))}",
            f"- gate_policy_change_recommended：{summary.get('gate_policy_change_recommended')}",
            f"- recommended_next_action：{summary.get('recommended_next_action')}",
            "",
            "结论：diagnostic gate 只用于归因，不修改正式 gate，也不触发 promotion。",
            "",
        ]
    )


def render_signal_vs_parameter_reader_brief(
    failure: Mapping[str, Any],
    shift: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Signal vs Parameter Attribution",
            "",
            f"- failure_source: {failure.get('failure_source')}",
            f"- confidence: {failure.get('confidence')}",
            f"- recommended_shift: {shift.get('recommended_shift')}",
            f"- next_task_family: {shift.get('next_task_family')}",
            "- safety: research_only / no official target / no broker / no production",
            "",
        ]
    )


def render_signal_vs_parameter_attribution_report(
    manifest: Mapping[str, Any],
    failure: Mapping[str, Any],
    shift: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Signal vs Parameter Attribution {manifest.get('attribution_id')}",
            "",
            f"- failure_source：{failure.get('failure_source')}",
            f"- confidence：{failure.get('confidence')}",
            f"- parameter_search_still_promising："
            f"{failure.get('parameter_search_still_promising')}",
            f"- signal_level_fix_required：{failure.get('signal_level_fix_required')}",
            f"- recommended_shift：{shift.get('recommended_shift')}",
            f"- next_task_family：{shift.get('next_task_family')}",
            "",
            "## Evidence",
            *[f"- {item}" for item in _texts(failure.get("evidence"))],
            "",
        ]
    )


def render_no_promotion_reader_brief(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    reasons = _records(summary.get("primary_reasons"))
    top_reason = _text(reasons[0].get("reason")) if reasons else "INSUFFICIENT_DATA"
    return "\n".join(
        [
            "## No-Promotion Review",
            "",
            f"- source_scorecard_id: {manifest.get('source_scorecard_id')}",
            f"- variants_reviewed: {manifest.get('variants_reviewed')}",
            f"- promoted_candidate_count: {manifest.get('promoted_candidate_count')}",
            f"- top_reason: {top_reason}",
            f"- gate_assessment: {summary.get('gate_assessment')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_no_promotion_review_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    failure: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> str:
    reasons = _records(summary.get("primary_reasons"))
    failures = _records(failure.get("failures"))
    components = _records(matrix.get("components"))
    reason_lines = [
        f"- {row.get('reason')}: count={row.get('variant_count')} severity={row.get('severity')}"
        for row in reasons
    ]
    failure_lines = [
        f"- {row.get('gate')}: "
        f"failed={row.get('failed_count')} near_miss={row.get('near_miss_count')}"
        for row in failures
    ]
    component_lines = [
        f"- {row.get('component')}: "
        f"avg={row.get('avg_score')} p90={row.get('p90_score')} "
        f"top={row.get('top_variant')}"
        for row in components[:8]
    ]
    return "\n".join(
        [
            f"# No-Promotion Review {manifest.get('review_id')}",
            "",
            f"- scorecard：{manifest.get('source_scorecard_id')}",
            f"- variants reviewed：{summary.get('variants_reviewed')}",
            f"- promoted candidates：{summary.get('promoted_candidate_count')}",
            f"- gate assessment：{summary.get('gate_assessment')}",
            f"- recommended next action：{summary.get('recommended_next_action')}",
            "",
            "## Primary Reasons",
            *reason_lines,
            "",
            "## Gate Failures",
            *failure_lines,
            "",
            "## Weak Components",
            *component_lines,
            "",
            "结论：本报告只解释 no-promotion 原因，不放宽 promotion gate，"
            "不生成 official target weights，不触发 broker 或 production。",
            "",
        ]
    )


def render_near_miss_reader_brief(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Near-Miss Candidates",
            "",
            f"- near_miss_id: {manifest.get('near_miss_id')}",
            f"- candidate_count: {manifest.get('candidate_count')}",
            f"- cash_buffer_10_near_miss: {manifest.get('cash_buffer_10_near_miss')}",
            f"- focus_families: {', '.join(_texts(summary.get('recommended_focus_families')))}",
            "- safety: research_only / no_official_target / no_broker / no_production",
            "",
        ]
    )


def render_near_miss_report(
    manifest: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    candidate_lines = [
        f"- rank {row.get('near_miss_rank')}: {row.get('variant_id')} "
        f"failed={','.join(_texts(row.get('failed_gates')))} "
        f"reason={row.get('near_miss_reason')}"
        for row in candidates[:10]
    ]
    return "\n".join(
        [
            f"# Near-Miss Candidates {manifest.get('near_miss_id')}",
            "",
            f"- source scorecard：{manifest.get('source_scorecard_id')}",
            f"- candidate count：{len(candidates)}",
            f"- focus families：{', '.join(_texts(summary.get('recommended_focus_families')))}",
            "",
            "## Top Near-Miss",
            *candidate_lines,
            "",
            "这些 candidates 只能进入 targeted v3 research search，"
            "不代表 promotion、owner approval 或 production readiness。",
            "",
        ]
    )


def render_cash_buffer_attribution_report(
    manifest: Mapping[str, Any],
    effect: Mapping[str, Any],
    failure: Mapping[str, Any],
    recommendations: Mapping[str, Any],
) -> str:
    improvements = _mapping(effect.get("improvements"))
    costs = _mapping(effect.get("costs"))
    return "\n".join(
        [
            f"# Cash Buffer Attribution {manifest.get('attribution_id')}",
            "",
            f"- variant：{manifest.get('variant_id')}",
            f"- promotion_failed：{failure.get('promotion_failed')}",
            f"- primary_failure_reason：{failure.get('primary_failure_reason')}",
            f"- overall_interpretation：{effect.get('overall_interpretation')}",
            "",
            "## Improvements",
            *[f"- {key}: {value}" for key, value in improvements.items()],
            "",
            "## Costs",
            *[f"- {key}: {value}" for key, value in costs.items()],
            "",
            f"- recommended_refinement：{', '.join(_texts(failure.get('recommended_refinement')))}",
            "- recommended_variants："
            f"{', '.join(_texts(recommendations.get('recommended_variants')))}",
            "",
        ]
    )


def render_search_coverage_gap_report(
    manifest: Mapping[str, Any],
    family_gap: Mapping[str, Any],
    parameter_gap: Mapping[str, Any],
    recommendations: Mapping[str, Any],
) -> str:
    parameter_lines = [
        f"- {row.get('parameter')}: "
        f"current={row.get('current_values')} "
        f"recommended={row.get('recommended_values')}"
        for row in _records(parameter_gap.get("gaps"))
    ]
    return "\n".join(
        [
            f"# Search Coverage Gap {manifest.get('coverage_gap_id')}",
            "",
            f"- search_space_id：{manifest.get('search_space_id')}",
            f"- near_miss_id：{manifest.get('near_miss_id')}",
            f"- recommended focus：{', '.join(_texts(recommendations.get('recommended_focus')))}",
            f"- max_v3_variants：{recommendations.get('max_v3_variants')}",
            "",
            "## Family Gaps",
            *[
                f"- {row.get('gap')}: status={row.get('status')} reason={row.get('reason')}"
                for row in _records(family_gap.get("gaps"))
            ],
            "",
            "## Parameter Gaps",
            *parameter_lines,
            "",
        ]
    )


def render_targeted_search_v3_report(
    manifest: Mapping[str, Any],
    coverage: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Targeted Search v3 Matrix {manifest.get('v3_matrix_id')}",
            "",
            f"- variants：{manifest.get('variant_count')}",
            f"- coverage_gap_id：{manifest.get('coverage_gap_id')}",
            f"- targeted families：{', '.join(_texts(coverage.get('targeted_families_covered')))}",
            "- every variant has a near_miss_parent or coverage_gap_reason",
            "- safety：experiment only / no official target / no broker / no production",
            "",
        ]
    )


def render_targeted_v3_backfill_report(
    manifest: Mapping[str, Any],
    progress: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Targeted v3 Backfill {manifest.get('v3_backfill_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- date range：{manifest.get('date_start')} -> {manifest.get('date_end')}",
            f"- data quality：{manifest.get('data_quality_status')}",
            f"- latest_valid_as_of：{manifest.get('latest_valid_as_of')}",
            "- variants completed："
            f"{progress.get('variants_completed')} / {progress.get('variants_total')}",
            "- safety：research screening only；no official target / no broker / no production",
            "",
        ]
    )


def render_near_miss_ab_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Near-Miss A/B Comparison {manifest.get('ab_id')}",
            "",
            f"- best_v3_variant：{summary.get('best_v3_variant')}",
            f"- v3_win_count：{summary.get('v3_win_count')}",
            f"- parent_win_count：{summary.get('parent_win_count')}",
            f"- inconclusive_count：{summary.get('inconclusive_count')}",
            "- safety：A/B result is diagnostics only, not promotion approval.",
            "",
        ]
    )


def render_threshold_sensitivity_report(
    manifest: Mapping[str, Any],
    scenarios: Sequence[Mapping[str, Any]],
    impact: Mapping[str, Any],
) -> str:
    scenario_lines = [
        f"- {row.get('scenario')}: "
        f"promote_count={row.get('promote_count')} "
        f"high_risk={row.get('high_risk_promote_count')} "
        f"recommended={row.get('recommended')}"
        for row in scenarios
    ]
    return "\n".join(
        [
            f"# Promotion Threshold Sensitivity {manifest.get('sensitivity_id')}",
            "",
            *scenario_lines,
            "",
            f"- relaxed_only_count：{len(_records(impact.get('relaxed_only_candidates')))}",
            "- rule：relaxed thresholds are diagnostics only and cannot auto-promote candidates.",
            "",
        ]
    )


def render_candidate_promotion_v2_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Candidate Promotion v2",
            "",
            f"- decision: {decision.get('decision')}",
            f"- promoted_count: {decision.get('promoted_count')}",
            f"- keep_testing_count: {decision.get('keep_testing_count')}",
            f"- recommended_next_action: {decision.get('recommended_next_action')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_candidate_promotion_v2_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Candidate Promotion Decision v2 {manifest.get('promotion_v2_id')}",
            "",
            f"- decision：{decision.get('decision')}",
            f"- promoted_count：{decision.get('promoted_count')}",
            f"- keep_testing_count：{decision.get('keep_testing_count')}",
            f"- rejected_count：{decision.get('rejected_count')}",
            f"- recommended_next_action：{decision.get('recommended_next_action')}",
            "- boundary：decision support only; no owner approval, no official weights, "
            "no broker, no production.",
            "",
        ]
    )


def render_owner_next_action_checklist(
    decision: Mapping[str, Any],
    formal: Mapping[str, Any],
    continue_plan: Mapping[str, Any],
) -> str:
    candidates = _records(formal.get("candidates"))
    actions = _texts(continue_plan.get("recommended_actions"))
    return "\n".join(
        [
            f"# Owner Next Action Checklist {decision.get('plan_id', '')}",
            "",
            f"- decision: {decision.get('decision')}",
            f"- recommended_next_action: {decision.get('recommended_next_action')}",
            f"- formal_candidate_count: {len(candidates)}",
            f"- continue_actions: {', '.join(actions)}",
            "- owner_review_required: true",
            "- confirm no official target weights are written",
            "- confirm broker_action_allowed=false",
            "- confirm production_effect=none",
            "",
        ]
    )


def render_next_plan_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Next Formal Or Search Plan",
            "",
            f"- decision: {decision.get('decision')}",
            f"- recommended_next_action: {decision.get('recommended_next_action')}",
            "- should_continue_parameter_search: "
            f"{decision.get('should_continue_parameter_search')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_next_formal_or_search_plan_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    formal: Mapping[str, Any],
    continue_plan: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Next Formal Or Search Plan {manifest.get('plan_id')}",
            "",
            f"- promotion_v2_id：{manifest.get('promotion_v2_id')}",
            f"- decision：{decision.get('decision')}",
            f"- recommended_next_action：{decision.get('recommended_next_action')}",
            f"- formal candidates：{len(_records(formal.get('candidates')))}",
            f"- continue actions：{', '.join(_texts(continue_plan.get('recommended_actions')))}",
            "- boundary：plan only；no official target / no broker / no production.",
            "",
        ]
    )


def render_weight_search_space_report(
    manifest: Mapping[str, Any], inventory: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Weight Search Space {manifest.get('search_space_id')}",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- 市场 regime：{manifest.get('market_regime')}",
            f"- 默认回测开始：{manifest.get('default_backtest_start')}",
            f"- families：{', '.join(_texts(manifest.get('families')))}",
            (
                "- initial max variants："
                f"{_mapping(manifest.get('max_variants')).get('initial_batch')}"
            ),
            (
                "- expanded max variants："
                f"{_mapping(manifest.get('max_variants')).get('expanded_batch')}"
            ),
            "- safety：research_screening_only / no official target / no broker / no production",
            "",
            "## Family Inventory",
            *[
                (
                    f"- {row.get('family')}: enabled={row.get('enabled')} "
                    f"parameters={row.get('parameter_count')}"
                )
                for row in _records(inventory.get("families"))
            ],
            "",
        ]
    )


def render_batch2_matrix_report(manifest: Mapping[str, Any], coverage: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Batch-2 Experiment Matrix {manifest.get('batch2_matrix_id')}",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- variants：{manifest.get('variant_count')}",
            f"- family coverage：{', '.join(_texts(coverage.get('families_covered')))}",
            (
                "- failure mode coverage："
                f"{len(_texts(coverage.get('failure_modes_covered')))} modes"
            ),
            (
                "- 结论边界：experiment only；不是 formal method、official target weights "
                "或 broker action。"
            ),
            "",
        ]
    )


def render_batch_backfill_report(manifest: Mapping[str, Any], progress: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Weight Batch Backfill {manifest.get('batch_backfill_id')}",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- 日期范围：{manifest.get('date_start')} -> {manifest.get('date_end')}",
            f"- data quality：{manifest.get('data_quality_status')}",
            f"- latest_valid_as_of：{manifest.get('latest_valid_as_of')}",
            f"- used_latest_valid_as_of：{manifest.get('used_latest_valid_as_of')}",
            (
                f"- variants completed：{progress.get('variants_completed')} / "
                f"{progress.get('variants_total')}"
            ),
            "- safety：no official target / no broker / no production",
            "",
        ]
    )


def render_weight_scorecard_report(
    manifest: Mapping[str, Any],
    distribution: Mapping[str, Any],
    pareto: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Weight Scorecard {manifest.get('scorecard_id')}",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- top return：{manifest.get('top_return_candidate')}",
            f"- top drawdown：{manifest.get('top_drawdown_candidate')}",
            f"- top stability：{manifest.get('top_stability_candidate')}",
            f"- Pareto candidates：{', '.join(_texts(pareto.get('candidates')))}",
            (
                f"- promote / keep / reject：{distribution.get('promote_count')} / "
                f"{distribution.get('keep_testing_count')} / {distribution.get('reject_count')}"
            ),
            "- safety：scorecard only；promotion gate 仍需人工 review，不触发 production。",
            "",
        ]
    )


def render_robustness_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Weight Robustness Review {manifest.get('robustness_id')}",
            "",
            f"- robust candidates：{', '.join(_texts(summary.get('robust_candidates')))}",
            f"- weak candidates：{', '.join(_texts(summary.get('weak_candidates')))}",
            f"- recommendation：{summary.get('recommended_next_action')}",
            "",
        ]
    )


def render_adaptive_branch_report(manifest: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Weight Adaptive Branch {manifest.get('branch_id')}",
            "",
            f"- branch decision：{decision.get('branch_decision')}",
            f"- reason：{'; '.join(_texts(decision.get('reason')))}",
            f"- next command：{decision.get('next_command')}",
            "",
        ]
    )


def render_candidate_cluster_report(
    manifest: Mapping[str, Any], representatives: Mapping[str, Any]
) -> str:
    representative_ids = ", ".join(
        _text(row.get("variant_id")) for row in _records(representatives.get("representatives"))
    )
    return "\n".join(
        [
            f"# Weight Candidate Cluster {manifest.get('cluster_id')}",
            "",
            f"- clusters：{manifest.get('cluster_count')}",
            f"- representatives：{representative_ids}",
            "",
        ]
    )


def render_top_candidate_interpretation_report(
    manifest: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Top Candidate Interpretation {manifest.get('interpretation_id')}",
            "",
            f"- recommended variant：{manifest.get('recommended_variant')}",
            *[
                f"- {row.get('variant_id')}: {', '.join(_texts(row.get('why_it_helped')))}"
                for row in explanations
            ],
            "",
        ]
    )


def render_top_candidate_reader_brief(
    manifest: Mapping[str, Any],
    explanations: Sequence[Mapping[str, Any]],
) -> str:
    top = _text(manifest.get("recommended_variant"), "INSUFFICIENT_DATA")
    return "\n".join(
        [
            "## Weight Batch Search Top Candidate",
            "",
            f"- recommended_variant: {top}",
            f"- interpreted_candidates: {len(explanations)}",
            "- safety: research_only / no_official_target / no_broker / no_production",
            "",
        ]
    )


def render_promotion_gate_report(manifest: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
    summary = _mapping(decision.get("decision_summary"))
    return "\n".join(
        [
            f"# Weight Method Promotion Gate {manifest.get('promotion_gate_id')}",
            "",
            f"- promoted：{summary.get('promoted_count')}",
            f"- keep_testing：{summary.get('keep_testing_count')}",
            f"- rejected：{summary.get('rejected_count')}",
            (
                "- safety：gate result is formal implementation eligibility only, "
                "not owner approval or production."
            ),
            "",
        ]
    )


def render_formal_method_implementation_plan(
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    validation_plan: Mapping[str, Any],
) -> str:
    lines = [
        f"# Formal Method Auto Plan {manifest.get('plan_id')}",
        "",
        f"- status: {manifest.get('status')}",
        f"- implemented: {manifest.get('implemented')}",
        "",
        "## Candidate Methods",
    ]
    for row in _records(specs.get("methods")):
        lines.append(
            f"- {row.get('method_name')}: complexity={row.get('implementation_complexity')}"
        )
    lines.extend(
        ["", "## Validation", f"- stages: {', '.join(_texts(validation_plan.get('stages')))}", ""]
    )
    return "\n".join(lines)


def render_formal_method_auto_plan_report(
    manifest: Mapping[str, Any], specs: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Formal Method Auto Plan Report {manifest.get('plan_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- method_count：{len(_records(specs.get('methods')))}",
            (
                "- TRADING-298～300：未实现 formal method 时保持 "
                "SKIPPED_NOT_IMPLEMENTED validation plan。"
            ),
            "",
        ]
    )


def render_dashboard_reader_brief(summary: Mapping[str, Any]) -> str:
    search = _mapping(summary.get("search_summary"))
    top = _mapping(summary.get("top_candidates"))
    next_actions = _mapping(summary.get("next_actions"))
    return "\n".join(
        [
            "## Weight Optimization Batch Search",
            "",
            f"- variants_total: {search.get('variants_total')}",
            f"- top_candidate: {top.get('top_overall_candidate')}",
            f"- branch_decision: {next_actions.get('branch_decision')}",
            f"- next_action: {next_actions.get('recommended_next_action')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_owner_decision_pack_report(
    manifest: Mapping[str, Any], options: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Owner Research Decision Pack {manifest.get('owner_pack_id')}",
            "",
            f"- recommended_decision：{options.get('recommended_decision')}",
            f"- available_options：{', '.join(_texts(options.get('available_options')))}",
            "- boundary：owner decision package 不能自动改 official target、broker 或 production。",
            "",
        ]
    )


def _generate_batch2_variants(config: Mapping[str, Any], *, expanded: bool) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    families = _mapping(config.get("families"))
    smoothing = _mapping(families.get("smoothing"))
    windows = [
        int(_float(value)) for value in _records_or_values(smoothing.get("windows"), [2, 3, 5, 7])
    ]
    alphas = [
        _float(value)
        for value in _records_or_values(smoothing.get("alpha"), [0.25, 0.35, 0.5, 0.65])
    ]
    max_changes = [
        _float(value)
        for value in _records_or_values(
            smoothing.get("max_daily_total_weight_change"), [0.04, 0.06, 0.08, 0.10]
        )
    ]
    if expanded:
        alphas = sorted(set([*alphas, 0.20, 0.30, 0.40, 0.55, 0.75]))
    for window in windows:
        for alpha in alphas:
            change = max_changes[(window + int(alpha * 100)) % len(max_changes)]
            variants.append(
                _variant(
                    f"smooth_{window}d_alpha_{int(alpha * 100)}_maxchg_{int(change * 100)}pct",
                    ["smoothing"],
                    [
                        {
                            "type": "weight_smoothing",
                            "window_days": window,
                            "alpha": alpha,
                            "max_daily_total_weight_change": change,
                        }
                    ],
                    ["weight_jump_high", "rolling_consistency_unstable", "turnover_high"],
                    ["lower_weight_jumps", "lower_signal_churn"],
                    ["may_lag_fast_regime_change"],
                )
            )
    cooldown = _mapping(families.get("cooldown"))
    cooldown_days = [
        int(_float(value))
        for value in _records_or_values(cooldown.get("cooldown_days"), [3, 5, 10])
    ]
    persistence_days = [
        int(_float(value))
        for value in _records_or_values(cooldown.get("min_signal_persistence"), [2, 3, 5])
    ]
    for days in cooldown_days:
        for persistence in persistence_days:
            variants.append(
                _variant(
                    f"sideways_cooldown_{days}d_persist_{persistence}d",
                    ["cooldown"],
                    [
                        {
                            "type": "regime_cooldown",
                            "regime": "sideways_choppy",
                            "cooldown_days": days,
                        },
                        {"type": "signal_persistence", "persistence_days": persistence},
                    ],
                    ["sideways_choppy_instability", "signal_churn"],
                    ["lower_signal_churn", "avoid_sideways_overtrading"],
                    ["may_delay_reentry"],
                )
            )
    gate_specs = [
        ("sideways_reduce_tilt_50", "sideways_choppy", "reduce_active_tilt", {"multiplier": 0.5}),
        ("sideways_hold_previous", "sideways_choppy", "hold_previous_weights", {}),
        ("tech_drawdown_block_risk_increase", "tech_drawdown", "block_risk_asset_increase", {}),
        (
            "semiconductor_pullback_block_smh_increase",
            "semiconductor_pullback",
            "block_symbol_increase",
            {"symbol": "SMH"},
        ),
        ("risk_off_only_allow_risk_reduction", "risk_off", "only_allow_risk_reduction", {}),
        (
            "strong_recovery_fast_restore",
            "strong_recovery",
            "reduce_active_tilt",
            {"multiplier": 0.85},
        ),
    ]
    for variant_id, regime, action, extra in gate_specs:
        variants.append(
            _variant(
                variant_id,
                ["regime_gating"],
                [{"type": "regime_gate", "regime": regime, "action": action, **extra}],
                ["regime_mismatch", "drawdown_not_improved"],
                ["improve_regime_specific_risk_control"],
                ["may_reduce_return_in_recovery"],
            )
        )
    thresholds = [0.02, 0.03, 0.05]
    for threshold in thresholds:
        variants.append(
            _variant(
                f"rebalance_delta_gt_{int(threshold * 100)}pct",
                ["rebalance_threshold"],
                [{"type": "rebalance_threshold", "min_total_abs_delta": threshold}],
                ["turnover_high", "weight_jump_high"],
                ["lower_turnover"],
                ["may_skip_small_useful_adjustments"],
            )
        )
    for method in [
        "median",
        "trimmed_mean",
        "weighted_mean",
        "top_3_candidate_consensus",
        "top_5_candidate_consensus",
        "cluster_representative_consensus",
        "risk_adjusted_weighted_consensus",
        "low_turnover_candidate_consensus",
    ]:
        variants.append(
            _variant(
                method.replace("_candidate_consensus", "_target_weights"),
                ["candidate_ensemble"],
                [{"type": "consensus_aggregation", "method": _consensus_method(method)}],
                ["rolling_consistency_unstable", "regime_mismatch"],
                ["reduce_single_candidate_noise"],
                ["may_blend_away_best_candidate"],
            )
        )
    for cash in [0.10, 0.15, 0.20]:
        variants.append(
            _variant(
                f"cash_buffer_{int(cash * 100)}",
                ["cash_buffer"],
                [{"type": "min_cash_weight", "min_cash_weight": cash}],
                ["drawdown_not_improved", "exposure_too_high"],
                ["lower_drawdown_pressure"],
                ["reduces_full_risk_asset_participation"],
            )
        )
    for cap in [0.20, 0.25, 0.30]:
        variants.append(
            _variant(
                f"semiconductor_cap_{int(cap * 100)}",
                ["risk_exposure_control"],
                [{"type": "cap_group_weight", "group": "semiconductor", "max_weight": cap}],
                ["higher_semiconductor_exposure", "exposure_too_high"],
                ["lower_semiconductor_concentration"],
                ["may_underperform_semiconductor_recovery"],
            )
        )
    for cap in [0.85, 0.90, 0.95]:
        variants.append(
            _variant(
                f"risk_asset_cap_{int(cap * 100)}",
                ["risk_exposure_control"],
                [{"type": "cap_group_weight", "group": "risk_assets", "max_weight": cap}],
                ["exposure_too_high", "drawdown_not_improved"],
                ["lower_total_risk_exposure"],
                ["may_reduce_return"],
            )
        )
    for cap in [0.04, 0.06, 0.08]:
        variants.append(
            _variant(
                f"turnover_cap_{int(cap * 100)}pct",
                ["turnover_control"],
                [{"type": "turnover_cap", "max_turnover": cap}],
                ["turnover_high", "weight_jump_high"],
                ["cap_rebalance_churn"],
                ["may_lag_signal_change"],
            )
        )
    hybrids = [
        (
            "smooth_3d_plus_rebalance_delta_3pct",
            ["smoothing", "rebalance_threshold"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.5},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.03},
            ],
        ),
        (
            "smooth_3d_plus_cash_buffer_15",
            ["smoothing", "cash_buffer"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.5},
                {"type": "min_cash_weight", "min_cash_weight": 0.15},
            ],
        ),
        (
            "smooth_3d_plus_tech_drawdown_block",
            ["smoothing", "regime_gating"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.5},
                {
                    "type": "regime_gate",
                    "regime": "tech_drawdown",
                    "action": "block_risk_asset_increase",
                },
            ],
        ),
        (
            "smooth_3d_plus_sideways_cooldown_5d",
            ["smoothing", "cooldown"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.5},
                {"type": "regime_cooldown", "regime": "sideways_choppy", "cooldown_days": 5},
            ],
        ),
        (
            "median_plus_rebalance_delta_3pct",
            ["candidate_ensemble", "rebalance_threshold"],
            [
                {"type": "consensus_aggregation", "method": "median"},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.03},
            ],
        ),
        (
            "top5_consensus_plus_smooth_3d",
            ["candidate_ensemble", "smoothing"],
            [
                {"type": "consensus_aggregation", "method": "weighted_mean"},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.5},
            ],
        ),
        (
            "sideways_hold_plus_strong_recovery_restore",
            ["regime_gating"],
            [
                {
                    "type": "regime_gate",
                    "regime": "sideways_choppy",
                    "action": "hold_previous_weights",
                },
                {
                    "type": "regime_gate",
                    "regime": "strong_recovery",
                    "action": "reduce_active_tilt",
                    "multiplier": 0.85,
                },
            ],
        ),
        (
            "cash15_plus_semiconductor_cap25",
            ["cash_buffer", "risk_exposure_control"],
            [
                {"type": "min_cash_weight", "min_cash_weight": 0.15},
                {"type": "cap_group_weight", "group": "semiconductor", "max_weight": 0.25},
            ],
        ),
        (
            "turnover_cap6_plus_rebalance_delta3",
            ["turnover_control", "rebalance_threshold"],
            [
                {"type": "turnover_cap", "max_turnover": 0.06},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.03},
            ],
        ),
        (
            "smooth_5d_plus_semiconductor_cap25",
            ["smoothing", "risk_exposure_control"],
            [
                {"type": "weight_smoothing", "window_days": 5, "alpha": 0.5},
                {"type": "cap_group_weight", "group": "semiconductor", "max_weight": 0.25},
            ],
        ),
    ]
    if expanded:
        hybrids.extend(
            [
                (
                    "smooth_2d_alpha40_plus_turnover_cap6",
                    ["smoothing", "turnover_control"],
                    [
                        {"type": "weight_smoothing", "window_days": 2, "alpha": 0.4},
                        {"type": "turnover_cap", "max_turnover": 0.06},
                    ],
                ),
                (
                    "smooth_7d_alpha65_plus_cash20",
                    ["smoothing", "cash_buffer"],
                    [
                        {"type": "weight_smoothing", "window_days": 7, "alpha": 0.65},
                        {"type": "min_cash_weight", "min_cash_weight": 0.20},
                    ],
                ),
                (
                    "trimmed_mean_plus_semiconductor_cap20",
                    ["candidate_ensemble", "risk_exposure_control"],
                    [
                        {"type": "consensus_aggregation", "method": "trimmed_mean"},
                        {"type": "cap_group_weight", "group": "semiconductor", "max_weight": 0.20},
                    ],
                ),
            ]
        )
    for variant_id, variant_families, transforms in hybrids:
        variants.append(
            _variant(
                variant_id,
                variant_families,
                transforms,
                ["weight_jump_high", "turnover_high", "rolling_consistency_unstable"],
                ["combine_complementary_controls"],
                ["compound_lag_or_return_sacrifice"],
            )
        )
    return _dedupe_variants(variants)


def _variant(
    variant_id: str,
    families: Sequence[str],
    transforms: Sequence[Mapping[str, Any]],
    failure_modes: Sequence[str],
    benefits: Sequence[str],
    costs: Sequence[str],
) -> dict[str, Any]:
    return {
        "variant_id": variant_id,
        "base_method": "limited_adjustment",
        "families": list(families),
        "family": families[0] if families else "UNKNOWN",
        "transforms": [dict(row) for row in transforms],
        "target_failure_modes": list(failure_modes),
        "expected_benefit": list(benefits),
        "expected_cost": list(costs),
        "complexity": "LOW" if len(transforms) <= 1 else "MEDIUM",
        "experiment_only": True,
        "research_screening_only": True,
        "not_formal_research_method": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": st.PRODUCTION_EFFECT,
        "auto_apply": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _scorecard_rows(
    backfill: Mapping[str, Any],
    variant_specs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    specs = {str(row.get("variant_id")): row for row in variant_specs}
    performance = {
        str(row.get("variant_id")): row
        for row in _records(backfill.get("variant_performance_metrics"))
    }
    stability = {
        str(row.get("variant_id")): row
        for row in _records(backfill.get("variant_stability_metrics"))
    }
    churn = {
        str(row.get("variant_id")): row for row in _records(backfill.get("variant_churn_metrics"))
    }
    lag = {str(row.get("variant_id")): row for row in _records(backfill.get("variant_lag_metrics"))}
    regimes = _records(backfill.get("variant_regime_metrics"))
    rows = []
    for variant_id, perf in performance.items():
        stable = _mapping(stability.get(variant_id))
        churn_row = _mapping(churn.get(variant_id))
        lag_row = _mapping(lag.get(variant_id))
        regime_rows = [row for row in regimes if row.get("variant_id") == variant_id]
        components = {
            "return": _bounded_score(
                _float(perf.get("relative_to_limited_adjustment")), -0.05, 0.05
            ),
            "annualized_return": _bounded_score(_float(perf.get("annualized_return")), -0.05, 0.25),
            "drawdown": _bounded_score(_float(perf.get("drawdown_delta_vs_limited")), -0.02, 0.02),
            "volatility": _bounded_score(-_float(perf.get("realized_volatility")), -0.35, -0.05),
            "risk_adjusted_return": _bounded_score(_risk_adjusted(perf), -1.0, 2.0),
            "turnover": _bounded_score(-_float(perf.get("turnover_delta_vs_limited")), -0.2, 0.2),
            "rolling_consistency": _label_score(
                _text(stable.get("rolling_consistency_delta")),
                {"IMPROVED": 1.0, "MIXED": 0.55, "INSUFFICIENT_DATA": 0.1, "WORSE": 0.0},
            ),
            "sideways_choppy": _regime_component(regime_rows, "sideways_choppy"),
            "tech_drawdown": _regime_component(regime_rows, "tech_drawdown"),
            "strong_recovery_lag": _label_score(
                _text(lag_row.get("lag_cost_status")),
                {"LOW": 1.0, "MEDIUM": 0.45, "HIGH": 0.0, "INSUFFICIENT_DATA": 0.2},
            ),
            "signal_churn": _bounded_score(-_float(churn_row.get("signal_churn_count")), -30, 0),
            "weight_jumps": _bounded_score(-_float(churn_row.get("large_jump_count")), -30, 0),
            "simplicity": _simplicity_score(_mapping(specs.get(variant_id))),
            "data_quality": 1.0 if backfill.get("data_quality_status") == "PASS" else 0.8,
        }
        overall = round(
            sum(components[key] * BATCH2_SCORE_WEIGHTS[key] for key in BATCH2_SCORE_WEIGHTS), 6
        )
        flags = _scorecard_hard_reject_flags(perf, stable, regime_rows, lag_row, backfill)
        decision = _scorecard_decision(overall, flags, perf, stable)
        spec = _mapping(specs.get(variant_id))
        rows.append(
            {
                "variant_id": variant_id,
                "families": _texts(spec.get("families")) or [_text(spec.get("family"))],
                "overall_score": overall,
                "score_components": {key: round(value, 6) for key, value in components.items()},
                "hard_reject_flags": flags,
                "scorecard_decision": decision,
                "total_return": perf.get("total_return"),
                "annualized_return": perf.get("annualized_return"),
                "max_drawdown": perf.get("max_drawdown"),
                "realized_volatility": perf.get("realized_volatility"),
                "turnover": perf.get("turnover"),
                "rolling_consistency_delta": stable.get("rolling_consistency_delta"),
                "reason": _scorecard_reason(decision, flags, perf, stable, lag_row),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(rows, key=lambda row: _float(row.get("overall_score")), reverse=True)


def _scorecard_hard_reject_flags(
    perf: Mapping[str, Any],
    stable: Mapping[str, Any],
    regime_rows: Sequence[Mapping[str, Any]],
    lag_row: Mapping[str, Any],
    backfill: Mapping[str, Any],
) -> list[str]:
    flags: list[str] = []
    if backfill.get("data_quality_status") == "FAIL":
        flags.append("data_quality_FAIL")
    if _float(perf.get("drawdown_delta_vs_limited")) < BATCH2_MATERIAL_DRAWDOWN_WORSE_DELTA:
        flags.append("max_drawdown_materially_worse_than_limited_adjustment")
    if stable.get("rolling_consistency_delta") == "WORSE":
        flags.append("rolling_consistency_worse_than_limited_adjustment")
    if _float(perf.get("turnover_delta_vs_limited")) > BATCH2_MATERIAL_TURNOVER_WORSE_DELTA:
        flags.append("turnover_materially_higher_than_limited_adjustment")
    if lag_row.get("lag_cost_status") == "HIGH":
        flags.append("strong_recovery_lag_cost_HIGH")
    if any(
        row.get("regime") == "sideways_choppy"
        and row.get("regime_status") == BATCH2_SIDWAYS_WORSE_REGIME_LABEL
        for row in regime_rows
    ):
        flags.append("sideways_choppy_performance_WORSE")
    pressure_worse = [
        row
        for row in regime_rows
        if row.get("regime") in {"tech_drawdown", "semiconductor_pullback", "risk_off"}
        and row.get("regime_status") == "WORSE"
    ]
    if len(pressure_worse) >= 2:
        flags.append("only_wins_in_one_narrow_window_or_pressure_regimes_worse")
    return flags


def _scorecard_decision(
    score: float,
    flags: Sequence[str],
    perf: Mapping[str, Any],
    stable: Mapping[str, Any],
) -> str:
    if flags:
        return "REJECT"
    if score >= BATCH2_PROMOTE_SCORE and perf.get("performance_status") != "FAIL":
        return "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    if score >= BATCH2_KEEP_TESTING_SCORE or stable.get("rolling_consistency_delta") == "IMPROVED":
        return "KEEP_FOR_MORE_TESTING"
    if perf.get("performance_status") == "INSUFFICIENT_DATA":
        return "DEFER_FOR_FORWARD_DATA"
    return "REJECT"


def _adaptive_branch_decision(
    scorecard: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(scorecard.get("variant_scorecard"))
    promoted = [
        row for row in rows if row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    ]
    robust = set(_texts(_mapping(robustness.get("robustness_summary")).get("robust_candidates")))
    promoted_robust = [row for row in promoted if row.get("variant_id") in robust or not robust]
    family_counts: dict[str, int] = {}
    for row in rows[:10]:
        for family in _texts(row.get("families")):
            family_counts[family] = family_counts.get(family, 0) + 1
    leading_family = max(family_counts, key=family_counts.get) if family_counts else "UNKNOWN"
    if scorecard.get("data_quality_status") == "FAIL":
        decision = "BLOCKED_DATA_QUALITY_FAIL"
        next_command = "aits validate-data"
        reason = ["data_quality_FAIL blocks research conclusion"]
    elif promoted_robust:
        decision = "RUN_PROMOTION_GATE"
        next_command = (
            "aits etf dynamic-v3-rescue weight-candidate-cluster run "
            "--scorecard-id <scorecard_id> --robustness-id <robustness_id>"
        )
        reason = [f"promote_count={len(promoted_robust)}", "no hard blockers in scorecard"]
    else:
        decision = "RUN_EXPANDED_SEARCH"
        next_command = (
            "aits etf dynamic-v3-rescue weight-expanded-search build --branch-id <branch_id>"
        )
        reason = ["no robust promotion candidate", f"leading_family={leading_family}"]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "branch_decision": decision,
        "leading_family": leading_family,
        "family_counts": family_counts,
        "reason": reason,
        "next_command": next_command,
        "search_space_id": scorecard.get("search_space_id", ""),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _candidate_clusters(
    scorecard: Mapping[str, Any],
    robustness: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    robust = set(_texts(_mapping(robustness.get("robustness_summary")).get("robust_candidates")))
    selected = [
        row
        for row in _records(scorecard.get("variant_scorecard"))
        if row.get("scorecard_decision")
        in {"PROMOTE_TO_FORMAL_IMPLEMENTATION", "KEEP_FOR_MORE_TESTING"}
    ][:20]
    if not selected:
        selected = _records(scorecard.get("variant_scorecard"))[:10]
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in selected:
        key = "+".join(sorted(_texts(row.get("families")) or ["UNKNOWN"]))
        groups.setdefault(key, []).append(row)
    clusters = []
    representatives = []
    for cluster_key, rows in sorted(groups.items()):
        ranked = sorted(
            rows,
            key=lambda row: (
                _text(row.get("variant_id")) not in robust,
                -_float(row.get("overall_score")),
            ),
        )
        representative = dict(ranked[0])
        representatives.append(
            {
                "cluster_id": cluster_key,
                "variant_id": representative.get("variant_id"),
                "families": representative.get("families"),
                "overall_score": representative.get("overall_score"),
                "scorecard_decision": representative.get("scorecard_decision"),
                "robustness_status": (
                    "ROBUST" if representative.get("variant_id") in robust else "REVIEW_REQUIRED"
                ),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
        clusters.append(
            {
                "cluster_id": cluster_key,
                "variant_count": len(rows),
                "member_variants": [_text(row.get("variant_id")) for row in rows],
                "representative_variant": representative.get("variant_id"),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return (
        {"schema_version": st.SCHEMA_VERSION, "clusters": clusters, **st.EXPERIMENT_FACTORY_SAFETY},
        {
            "schema_version": st.SCHEMA_VERSION,
            "representatives": representatives,
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
    )


def _candidate_explanation(row: Mapping[str, Any]) -> dict[str, Any]:
    families = _texts(row.get("families"))
    score = _float(row.get("overall_score"))
    decision = _text(row.get("scorecard_decision"))
    return {
        "variant_id": row.get("variant_id"),
        "families": families,
        "scorecard_decision": decision,
        "overall_score": score,
        "what_it_changes": [f"adjusts {family}" for family in families],
        "why_it_helped": _family_benefits(families),
        "what_it_costs": _family_costs(families),
        "best_regimes": ["sideways_choppy" if "cooldown" in families else "ai_after_chatgpt"],
        "weak_regimes": [
            (
                "strong_recovery"
                if {"smoothing", "cooldown"} & set(families)
                else "requires_forward_confirmation"
            )
        ],
        "recommended_promotion": decision == "PROMOTE_TO_FORMAL_IMPLEMENTATION",
        "implementation_complexity": "LOW" if len(families) <= 1 else "MEDIUM",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _promotion_gate_decisions(explanations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in explanations:
        if row.get("recommended_promotion") is True:
            decision = "PROMOTE_TO_FORMAL_IMPLEMENTATION"
        elif row.get("scorecard_decision") == "KEEP_FOR_MORE_TESTING":
            decision = "KEEP_FOR_MORE_TESTING"
        elif "requires_forward_confirmation" in _texts(row.get("weak_regimes")):
            decision = "DEFER_FOR_FORWARD_DATA"
        else:
            decision = "REJECT"
        rows.append(
            {
                "variant_id": row.get("variant_id"),
                "families": row.get("families"),
                "decision": decision,
                "reason": [
                    f"scorecard_decision={row.get('scorecard_decision')}",
                    "research_only_no_owner_approval_no_production",
                ],
                "implementation_complexity": row.get("implementation_complexity", "MEDIUM"),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _formal_method_specs(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    methods = []
    for row in candidates:
        variant_id = _text(row.get("variant_id"))
        method_name = f"{variant_id}_limited_adjustment_research_method"
        methods.append(
            {
                "variant_id": variant_id,
                "method_name": method_name,
                "implementation_scope": "research_only",
                "implementation_complexity": row.get("implementation_complexity", "MEDIUM"),
                "transform_composable": True,
                "implementation_executed": False,
                "research_target_only": True,
                "not_official_target_weights": True,
                "paper_shadow_only": True,
                "broker_action_allowed": False,
                "production_effect": st.PRODUCTION_EFFECT,
                "auto_apply": False,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": st.SCHEMA_VERSION, "methods": methods, **st.EXPERIMENT_FACTORY_SAFETY}


def _formal_validation_plan(specs: Mapping[str, Any]) -> dict[str, Any]:
    implemented = any(
        row.get("implementation_executed") is True for row in _records(specs.get("methods"))
    )
    status = "READY_AFTER_IMPLEMENTATION" if implemented else "SKIPPED_NOT_IMPLEMENTED"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "stages": [
            "TRADING-298 formal candidate paper shadow backfill",
            "TRADING-299 formal candidate comparison and hardening review",
            "TRADING-300 forward confirmation registration",
        ],
        "stage_status": {
            "formal_candidate_paper_shadow_backfill": status,
            "formal_candidate_comparison_hardening": status,
            "forward_confirmation_registration": status,
        },
        "skip_reason": (
            "formal method was not implemented in this auto-plan artifact"
            if not implemented
            else ""
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _dashboard_summary(
    scorecard: Mapping[str, Any],
    branch: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(scorecard.get("variant_scorecard"))
    promoted = [
        row
        for row in _records(_mapping(gate.get("promotion_gate_decision")).get("decisions"))
        if row.get("decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    ]
    rejected = [row for row in rows if row.get("scorecard_decision") == "REJECT"]
    branch_payload = _mapping(branch.get("branch_decision_payload"))
    return {
        "search_summary": {
            "schema_version": st.SCHEMA_VERSION,
            "variants_total": len(rows),
            "data_quality_status": scorecard.get("data_quality_status"),
            "families_ranked": _rank_families(rows),
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        "top_candidates": {
            "schema_version": st.SCHEMA_VERSION,
            "top_overall_candidate": _text(rows[0].get("variant_id")) if rows else "",
            "top_return_candidate": scorecard.get("top_return_candidate"),
            "top_drawdown_candidate": scorecard.get("top_drawdown_candidate"),
            "top_stability_candidate": scorecard.get("top_stability_candidate"),
            "promoted_candidates": [_text(row.get("variant_id")) for row in promoted],
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        "rejected_summary": {
            "schema_version": st.SCHEMA_VERSION,
            "rejected_count": len(rejected),
            "top_reject_reasons": _top_reject_reasons(rejected),
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        "next_actions": {
            "schema_version": st.SCHEMA_VERSION,
            "branch_decision": branch_payload.get("branch_decision"),
            "recommended_next_action": (
                "implement_top_candidate" if promoted else "run_expanded_search"
            ),
            "no_official_target_no_broker_no_production": True,
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
    }


def _owner_decision_options(dashboard: Mapping[str, Any]) -> dict[str, Any]:
    top = _mapping(dashboard.get("top_candidates"))
    next_actions = _mapping(dashboard.get("next_actions"))
    promoted = _texts(top.get("promoted_candidates"))
    if promoted:
        recommended = "implement_top_candidate"
    elif next_actions.get("branch_decision") == "RUN_EXPANDED_SEARCH":
        recommended = "run_expanded_search"
    else:
        recommended = "continue_search"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "available_options": [
            "continue_search",
            "implement_top_candidate",
            "defer_for_forward_data",
            "reject_all_candidates",
            "run_expanded_search",
        ],
        "recommended_decision": recommended,
        "reason": [
            f"promoted_candidates={len(promoted)}",
            f"branch_decision={next_actions.get('branch_decision')}",
            "owner pack is decision support only",
        ],
        "production_actions_allowed": False,
        "broker_action_allowed": False,
        "official_target_weights_allowed": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _micro_search_v4_design_rationale(
    gate: Mapping[str, Any],
    attribution: Mapping[str, Any],
    signal: Mapping[str, Any],
    consensus: Mapping[str, Any],
) -> dict[str, Any]:
    distribution = _mapping(attribution.get("score_component_distribution"))
    signal_summary = _mapping(signal.get("signal_instability_summary"))
    consensus_failure = _mapping(consensus.get("consensus_failure_reasons"))
    gate_diagnosis = _mapping(gate.get("gate_strictness_diagnosis"))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "design_principles": [
            "limit variants to a focused 20-40 micro search",
            "prefer signal and consensus hypotheses over blind parameter expansion",
            "keep every variant experiment-only and not official target weights",
        ],
        "recommended_focus": [
            "cash_buffer_10 near-miss refinements",
            "smooth_weights_3d refinements",
            "median/top-k consensus refinements",
            "dispersion gate and high-disagreement hold",
            "sideways hold plus fast restore",
        ],
        "gate_assessment": gate_diagnosis.get("calibrated_assessment"),
        "dominant_weak_components": _texts(distribution.get("dominant_weak_components")),
        "dominant_signal_issue": signal_summary.get("dominant_signal_issue"),
        "consensus_failure_reason": consensus_failure.get("primary_failure_reason"),
        "variant_count_target": [V4_MICRO_MIN_VARIANTS, V4_MICRO_MAX_VARIANTS],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _micro_search_v4_variant_specs(rationale: Mapping[str, Any]) -> list[dict[str, Any]]:
    _ = rationale
    specs = [
        (
            "smooth_3d_plus_dispersion_gate",
            "smooth_weights_3d_limited_adjustment",
            ["smoothing", "candidate_ensemble"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "hold_previous_weights",
                },
            ],
            ["candidate_disagreement", "signal_churn", "rolling_consistency_unstable"],
        ),
        (
            "smooth_3d_plus_topk_stability_filter",
            "smooth_weights_3d_limited_adjustment",
            ["smoothing", "candidate_ensemble"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
                {"type": "topk_stability_filter", "top_k": 5, "min_overlap": 3},
            ],
            ["unstable_topk", "candidate_disagreement"],
        ),
        (
            "smooth_3d_plus_rebalance_delta_3pct",
            "smooth_weights_3d_limited_adjustment",
            ["smoothing", "rebalance_threshold"],
            [
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.03},
            ],
            ["signal_churn", "weight_jump_high"],
        ),
        (
            "cash_buffer_8_plus_smooth_3d",
            "limited_adjustment",
            ["cash_buffer", "smoothing"],
            [
                {"type": "min_cash_weight", "min_cash_weight": 0.08},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
            ],
            ["return_preservation_weak", "drawdown_gate"],
        ),
        (
            "cash_buffer_10_plus_dispersion_gate",
            "limited_adjustment",
            ["cash_buffer", "candidate_ensemble"],
            [
                {"type": "min_cash_weight", "min_cash_weight": 0.10},
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "hold_previous_weights",
                },
            ],
            ["candidate_disagreement", "regime_mismatch"],
        ),
        (
            "median_consensus_plus_smooth_3d",
            "median_target_weights",
            ["candidate_ensemble", "smoothing"],
            [
                {"type": "consensus_aggregation", "method": "median"},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
            ],
            ["over_averaging", "signal_churn"],
        ),
        (
            "median_consensus_plus_dispersion_gate",
            "median_target_weights",
            ["candidate_ensemble"],
            [
                {"type": "consensus_aggregation", "method": "median"},
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "hold_previous_weights",
                },
            ],
            ["candidate_disagreement"],
        ),
        (
            "top5_consensus_plus_smooth_3d",
            "top5_candidate_consensus",
            ["candidate_ensemble", "smoothing"],
            [
                {"type": "candidate_subset", "top_k": 5},
                {"type": "consensus_aggregation", "method": "weighted_mean"},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
            ],
            ["unstable_topk", "signal_churn"],
        ),
        (
            "top5_consensus_plus_rebalance_threshold",
            "top5_candidate_consensus",
            ["candidate_ensemble", "rebalance_threshold"],
            [
                {"type": "candidate_subset", "top_k": 5},
                {"type": "consensus_aggregation", "method": "weighted_mean"},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.03},
            ],
            ["weight_jump_high", "unstable_topk"],
        ),
        (
            "high_disagreement_hold_previous",
            "limited_adjustment",
            ["candidate_ensemble", "regime_gating"],
            [
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "hold_previous_weights",
                }
            ],
            ["candidate_disagreement", "false_risk_on"],
        ),
        (
            "high_disagreement_reduce_tilt_50",
            "limited_adjustment",
            ["candidate_ensemble", "risk_exposure_control"],
            [
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "reduce_active_tilt",
                    "multiplier": 0.50,
                }
            ],
            ["candidate_disagreement", "false_risk_on"],
        ),
        (
            "sideways_hold_plus_fast_restore",
            "limited_adjustment",
            ["regime_gating"],
            [
                {
                    "type": "regime_gate",
                    "regime": "sideways_choppy",
                    "action": "hold_previous_weights",
                },
                {
                    "type": "regime_gate",
                    "regime": "strong_recovery",
                    "action": "reduce_active_tilt",
                    "multiplier": 0.90,
                },
            ],
            ["sideways_choppy", "late_response"],
        ),
    ]
    specs.extend(_micro_search_v4_extra_specs())
    variants = []
    for variant_id, base_method, families, transforms, failure_modes in specs:
        variant = _variant(
            variant_id,
            families,
            transforms,
            failure_modes,
            ["targeted micro search around diagnosed gate/signal/consensus weakness"],
            ["may reduce recovery return or fail to improve composite score"],
        )
        variant["base_method"] = base_method
        variant["rationale"] = "TRADING-316_to_325 diagnostic micro search variant"
        variants.append(variant)
    return _dedupe_variants(variants)[:V4_MICRO_MAX_VARIANTS]


def _micro_search_v4_extra_specs() -> list[
    tuple[str, str, list[str], list[dict[str, Any]], list[str]]
]:
    return [
        (
            "cash_buffer_6_plus_smooth_3d",
            "limited_adjustment",
            ["cash_buffer", "smoothing"],
            [
                {"type": "min_cash_weight", "min_cash_weight": 0.06},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
            ],
            ["return_preservation_weak"],
        ),
        (
            "cash_buffer_8_plus_rebalance_delta_3pct",
            "limited_adjustment",
            ["cash_buffer", "rebalance_threshold"],
            [
                {"type": "min_cash_weight", "min_cash_weight": 0.08},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.03},
            ],
            ["weight_jump_high"],
        ),
        (
            "cash_buffer_10_plus_rebalance_delta_25bp",
            "limited_adjustment",
            ["cash_buffer", "rebalance_threshold"],
            [
                {"type": "min_cash_weight", "min_cash_weight": 0.10},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.025},
            ],
            ["composite_score_gate"],
        ),
        (
            "median_consensus_plus_rebalance_delta_25bp",
            "median_target_weights",
            ["candidate_ensemble", "rebalance_threshold"],
            [
                {"type": "consensus_aggregation", "method": "median"},
                {"type": "rebalance_threshold", "min_total_abs_delta": 0.025},
            ],
            ["over_averaging"],
        ),
        (
            "trimmed_mean_consensus_plus_smooth_3d",
            "trimmed_mean_target_weights",
            ["candidate_ensemble", "smoothing"],
            [
                {"type": "consensus_aggregation", "method": "trimmed_mean"},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
            ],
            ["candidate_disagreement"],
        ),
        (
            "top3_consensus_plus_dispersion_gate",
            "top_3_candidate_consensus",
            ["candidate_ensemble"],
            [
                {"type": "candidate_subset", "top_k": 3},
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_MODERATE_DISPERSION,
                    "action": "hold_previous_weights",
                },
            ],
            ["unstable_topk"],
        ),
        (
            "top5_consensus_plus_dispersion_gate",
            "top5_candidate_consensus",
            ["candidate_ensemble"],
            [
                {"type": "candidate_subset", "top_k": 5},
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "hold_previous_weights",
                },
            ],
            ["candidate_disagreement"],
        ),
        (
            "smooth_2d_alpha40_plus_dispersion_gate",
            "smooth_weights_3d_limited_adjustment",
            ["smoothing", "candidate_ensemble"],
            [
                {"type": "weight_smoothing", "window_days": 2, "alpha": 0.40},
                {
                    "type": "dispersion_gate",
                    "max_candidate_dispersion": CONSENSUS_HIGH_DISPERSION,
                    "action": "hold_previous_weights",
                },
            ],
            ["signal_churn"],
        ),
        (
            "smooth_4d_alpha50_plus_fast_restore",
            "smooth_weights_3d_limited_adjustment",
            ["smoothing", "regime_gating"],
            [
                {"type": "weight_smoothing", "window_days": 4, "alpha": 0.50},
                {
                    "type": "regime_gate",
                    "regime": "strong_recovery",
                    "action": "reduce_active_tilt",
                    "multiplier": 0.95,
                },
            ],
            ["late_response"],
        ),
        (
            "sideways_hold_plus_cash_buffer_8",
            "limited_adjustment",
            ["regime_gating", "cash_buffer"],
            [
                {
                    "type": "regime_gate",
                    "regime": "sideways_choppy",
                    "action": "hold_previous_weights",
                },
                {"type": "min_cash_weight", "min_cash_weight": 0.08},
            ],
            ["sideways_choppy", "drawdown_gate"],
        ),
        (
            "risk_tilt_reduce_25_plus_fast_restore",
            "limited_adjustment",
            ["risk_exposure_control", "regime_gating"],
            [
                {"type": "cap_group_weight", "group": "risk_assets", "max_weight": 0.90},
                {
                    "type": "regime_gate",
                    "regime": "strong_recovery",
                    "action": "reduce_active_tilt",
                    "multiplier": 0.95,
                },
            ],
            ["false_risk_on", "late_response"],
        ),
        (
            "semiconductor_cap25_plus_smooth_3d",
            "limited_adjustment",
            ["risk_exposure_control", "smoothing"],
            [
                {"type": "cap_group_weight", "group": "semiconductor", "max_weight": 0.25},
                {"type": "weight_smoothing", "window_days": 3, "alpha": 0.50},
            ],
            ["regime_mismatch", "weight_jump_high"],
        ),
    ]


def _v4_variant_signal_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    stability_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    churn = _variant_churn_metrics(variant_states, stability_rows)
    churn_by_id = {_text(row.get("variant_id")): row for row in churn}
    regime_by_id: dict[str, list[Mapping[str, Any]]] = {}
    for row in regime_rows:
        regime_by_id.setdefault(_text(row.get("variant_id")), []).append(row)
    rows = []
    for variant_id in sorted(churn_by_id):
        churn_row = _mapping(churn_by_id.get(variant_id))
        regimes = regime_by_id.get(variant_id, [])
        worse = [row for row in regimes if row.get("regime_status") == "WORSE"]
        rows.append(
            {
                "variant_id": variant_id,
                "signal_churn_count": churn_row.get("signal_churn_count", 0),
                "large_jump_count": churn_row.get("large_jump_count", 0),
                "large_weight_jump_count": churn_row.get("large_jump_count", 0),
                "regime_mismatch_count": len(worse),
                "false_risk_on_count": sum(
                    1 for row in worse if row.get("regime") in {"tech_drawdown", "risk_off"}
                ),
                "false_risk_off_count": sum(
                    1 for row in worse if row.get("regime") == "strong_recovery"
                ),
                "signal_metric_status": "REVIEW_REQUIRED" if worse else "PASS",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _v4_scorecard_rows(
    backfill: Mapping[str, Any],
    design: Mapping[str, Any],
) -> list[dict[str, Any]]:
    payload = {
        "data_quality_status": backfill.get("data_quality_status"),
        "variant_performance_metrics": backfill.get("v4_variant_performance"),
        "variant_stability_metrics": backfill.get("v4_variant_stability_metrics"),
        "variant_churn_metrics": backfill.get("v4_variant_signal_metrics"),
        "variant_lag_metrics": _variant_lag_metrics(backfill.get("v4_variant_regime_metrics", [])),
        "variant_regime_metrics": backfill.get("v4_variant_regime_metrics"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return _scorecard_rows(payload, _records(design.get("v4_variant_specs")))


def _gate_review_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    diagnostic: bool,
) -> list[dict[str, Any]]:
    threshold = BATCH2_PROMOTE_SCORE - (GATE_DIAGNOSTIC_RELAXATION if diagnostic else 0.0)
    result = []
    for row in rows:
        promoted = _float(row.get("overall_score")) >= threshold and not _high_risk_gate_failure(
            row
        )
        result.append(
            {
                "variant_id": row.get("variant_id"),
                "overall_score": row.get("overall_score"),
                "gate_track": (
                    "diagnostic_calibrated_gate" if diagnostic else "official_research_gate"
                ),
                "promoted": promoted,
                "candidate_status": (
                    "DIAGNOSTIC_ONLY_PROMOTED"
                    if diagnostic and promoted
                    else "PROMOTED"
                    if promoted
                    else "REJECTED"
                ),
                "failed_gates": _failed_gates(row),
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                "production_effect": st.PRODUCTION_EFFECT,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return result


def _gate_calibrated_summary(
    official: Sequence[Mapping[str, Any]],
    diagnostic: Sequence[Mapping[str, Any]],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    official_promoted = {_text(row.get("variant_id")) for row in official if row.get("promoted")}
    diagnostic_promoted = {
        _text(row.get("variant_id")) for row in diagnostic if row.get("promoted")
    }
    diagnostic_only = sorted(diagnostic_promoted - official_promoted)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "official_gate_promoted_count": len(official_promoted),
        "diagnostic_gate_promoted_count": len(diagnostic_promoted),
        "diagnostic_only_candidates": diagnostic_only,
        "gate_policy_change_recommended": False,
        "recommended_next_action": "signal_vs_parameter_attribution",
        "source_gate_calibrated_assessment": _mapping(gate.get("gate_strictness_diagnosis")).get(
            "calibrated_assessment"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _signal_vs_parameter_failure_source(
    signal: Mapping[str, Any],
    consensus: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    signal_summary = _mapping(signal.get("signal_instability_summary"))
    consensus_failure = _mapping(consensus.get("consensus_failure_reasons"))
    gate_summary = _mapping(gate.get("gate_calibrated_summary"))
    evidence = [
        f"dominant_signal_issue={signal_summary.get('dominant_signal_issue')}",
        f"consensus_failure_reason={consensus_failure.get('primary_failure_reason')}",
        f"official_gate_promoted_count={gate_summary.get('official_gate_promoted_count')}",
        f"diagnostic_gate_promoted_count={gate_summary.get('diagnostic_gate_promoted_count')}",
    ]
    if signal_summary.get("requires_signal_level_fix") is True:
        source = "SIGNAL_QUALITY"
        confidence = "HIGH" if gate_summary.get("diagnostic_gate_promoted_count") == 0 else "MEDIUM"
        signal_fix = True
        parameter_promising = False
    elif consensus_failure.get("primary_failure_reason") in {
        "candidate_disagreement",
        "over_averaging",
        "poor_topk_selection",
    }:
        source = "CONSENSUS_QUALITY"
        confidence = "MEDIUM"
        signal_fix = True
        parameter_promising = False
    elif int(_float(gate_summary.get("diagnostic_gate_promoted_count"))) > int(
        _float(gate_summary.get("official_gate_promoted_count"))
    ):
        source = "GATE_POLICY"
        confidence = "MEDIUM"
        signal_fix = False
        parameter_promising = True
    else:
        source = "MARKET_REGIME"
        confidence = "LOW"
        signal_fix = False
        parameter_promising = False
    return {
        "schema_version": st.SCHEMA_VERSION,
        "failure_source": source,
        "confidence": confidence,
        "evidence": evidence,
        "parameter_search_still_promising": parameter_promising,
        "signal_level_fix_required": signal_fix,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _recommended_research_shift(
    failure: Mapping[str, Any],
    consensus: Mapping[str, Any],
) -> dict[str, Any]:
    source = _text(failure.get("failure_source"))
    consensus_failure = _mapping(consensus.get("consensus_failure_reasons"))
    if source == "SIGNAL_QUALITY":
        shift = "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS"
        task_family = "signal_feature_diagnosis"
    elif source == "CONSENSUS_QUALITY":
        shift = "SHIFT_TO_CANDIDATE_QUALITY_FILTER"
        task_family = "candidate_quality_filter"
    elif source == "GATE_POLICY":
        shift = "REVIEW_GATE_POLICY"
        task_family = "gate_policy_review"
    elif failure.get("parameter_search_still_promising") is True:
        shift = "CONTINUE_MICRO_SEARCH"
        task_family = "micro_search_v5"
    else:
        shift = "DEFER"
        task_family = "signal_feature_diagnosis"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "recommended_shift": shift,
        "reason": [
            f"failure_source={source}",
            f"consensus_failure={consensus_failure.get('primary_failure_reason')}",
        ],
        "next_task_family": task_family,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _no_promotion_reason_summary(scorecard: Mapping[str, Any]) -> dict[str, Any]:
    rows = _records(scorecard.get("variant_scorecard"))
    promoted = [
        row for row in rows if row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    ]
    distribution = _gate_failure_distribution(rows)
    component_matrix = _score_component_failure_matrix(rows)
    primary = []
    for row in _records(distribution.get("failures")):
        count = int(_float(row.get("failed_count")))
        if count <= 0:
            continue
        primary.append(
            {
                "reason": _reason_for_gate(_text(row.get("gate"))),
                "variant_count": count,
                "severity": _gate_failure_severity(_text(row.get("gate")), count, len(rows)),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    if not primary:
        weak = [
            row
            for row in _records(component_matrix.get("components"))
            if _float(row.get("avg_score")) < 0.50
        ]
        primary = [
            {
                "reason": f"{row.get('component')}_weak",
                "variant_count": len(rows),
                "severity": "MEDIUM",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for row in weak[:3]
        ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "review_id": "",
        "source_scorecard_id": scorecard.get("scorecard_id"),
        "variants_reviewed": len(rows),
        "promoted_candidate_count": len(promoted),
        "primary_reasons": primary[:5],
        "gate_assessment": _gate_assessment(scorecard, distribution),
        "recommended_next_action": "extract_near_miss_candidates",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _gate_failure_distribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    failures = []
    for gate in PROMOTION_GATE_UNIVERSE:
        failed = [row for row in rows if gate in _failed_gates(row)]
        near_miss = [row for row in failed if _is_near_miss_candidate(row)]
        failures.append(
            {
                "gate": gate,
                "failed_count": len(failed),
                "near_miss_count": len(near_miss),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "failures": failures,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _score_component_failure_matrix(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    component_names = sorted(
        {key for row in rows for key in _mapping(row.get("score_components")).keys()}
    )
    components = []
    for component in component_names:
        values = [_float(_mapping(row.get("score_components")).get(component)) for row in rows]
        ranked = sorted(
            rows,
            key=lambda row: _float(_mapping(row.get("score_components")).get(component)),
            reverse=True,
        )
        components.append(
            {
                "component": component,
                "avg_score": round(sum(values) / len(values), 6) if values else 0.0,
                "p90_score": round(_percentile(values, 0.90), 6),
                "top_variant": _text(ranked[0].get("variant_id")) if ranked else "",
                "weak_count": sum(1 for value in values if value < 0.50),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "components": sorted(components, key=lambda row: _float(row.get("avg_score"))),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _gate_assessment(scorecard: Mapping[str, Any], distribution: Mapping[str, Any]) -> str:
    rows = _records(scorecard.get("variant_scorecard"))
    if not rows or scorecard.get("data_quality_status") == "FAIL":
        return "INCONCLUSIVE"
    max_score = max(_float(row.get("overall_score")) for row in rows)
    near_miss_count = sum(1 for row in rows if _is_near_miss_candidate(row))
    severe_failure_count = sum(
        int(_float(row.get("failed_count")))
        for row in _records(distribution.get("failures"))
        if row.get("gate") in {"drawdown_gate", "regime_gate", "recovery_lag_gate"}
    )
    if max_score >= BATCH2_PROMOTE_SCORE - NO_PROMOTION_NEAR_MISS_MARGIN and near_miss_count >= 3:
        return "TOO_STRICT"
    if severe_failure_count >= max(1, len(rows) // 2):
        return "REASONABLE"
    if max_score < BATCH2_KEEP_TESTING_SCORE:
        return "REASONABLE"
    return "INCONCLUSIVE"


def _near_miss_candidate_rows(scorecard: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = []
    for row in _records(scorecard.get("variant_scorecard")):
        if not _is_near_miss_candidate(row):
            continue
        failed = _failed_gates(row)
        candidates.append(
            {
                "variant_id": row.get("variant_id"),
                "family": _texts(row.get("families"))[0]
                if _texts(row.get("families"))
                else "UNKNOWN",
                "families": _texts(row.get("families")),
                "overall_score": row.get("overall_score"),
                "near_miss_rank": 0,
                "passed_gates": _passed_gates(row),
                "failed_gates": failed,
                "near_miss_reason": _near_miss_reason(row, failed),
                "suggested_adjustment": _suggested_adjustment(row, failed),
                "candidate_status": "NEAR_MISS",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    for rank, row in enumerate(
        sorted(candidates, key=lambda item: _float(item.get("overall_score")), reverse=True),
        start=1,
    ):
        row["near_miss_rank"] = rank
    return candidates[:20]


def _is_near_miss_candidate(row: Mapping[str, Any]) -> bool:
    if row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION":
        return False
    failed = _failed_gates(row)
    components = _mapping(row.get("score_components"))
    strong_component = max((_float(value) for value in components.values()), default=0.0)
    return len(failed) <= NEAR_MISS_MAX_FAILED_GATES and (
        _float(row.get("overall_score")) >= NEAR_MISS_MIN_OVERALL_SCORE
        or strong_component >= NEAR_MISS_MIN_COMPONENT_SCORE
    )


def _failed_gates(row: Mapping[str, Any]) -> list[str]:
    gates = []
    for flag in _texts(row.get("hard_reject_flags")):
        gate = HARD_REJECT_GATE_MAP.get(flag)
        if gate and gate not in gates:
            gates.append(gate)
    components = _mapping(row.get("score_components"))
    if _float(row.get("overall_score")) < BATCH2_PROMOTE_SCORE:
        gates.append("composite_score_gate")
    if _float(components.get("return")) < 0.45:
        gates.append("return_preservation_gate")
    return [gate for gate in PROMOTION_GATE_UNIVERSE if gate in set(gates)]


def _passed_gates(row: Mapping[str, Any]) -> list[str]:
    failed = set(_failed_gates(row))
    return [gate for gate in PROMOTION_GATE_UNIVERSE if gate not in failed]


def _near_miss_reason(row: Mapping[str, Any], failed: Sequence[str]) -> str:
    families = set(_texts(row.get("families")))
    if "cash_buffer" in families and "return_preservation_gate" in failed:
        return "strong_drawdown_but_weak_return"
    if "smoothing" in families and "recovery_lag_gate" in failed:
        return "smoothing_helped_stability_but_recovery_lagged"
    if "composite_score_gate" in failed and len(failed) == 1:
        return "below_promotion_score_but_no_hard_reject"
    if failed:
        return "limited_gate_failures_with_positive_components"
    return "requires_forward_confirmation"


def _suggested_adjustment(row: Mapping[str, Any], failed: Sequence[str]) -> str:
    families = set(_texts(row.get("families")))
    if "cash_buffer" in families:
        return "test_cash_buffer_8_or_hybrid_with_smoothing"
    if "smoothing" in families:
        return "test_shorter_smoothing_or_fast_restore_hybrid"
    if "candidate_ensemble" in families:
        return "test_top_k_consensus_with_threshold"
    if "rebalance_threshold" in families:
        return "test_threshold_2pct_to_4pct_grid"
    if "return_preservation_gate" in failed:
        return "reduce_defensive_drag_or_add_recovery_restore"
    return "targeted_v3_hybrid_search"


def _near_miss_family_summary(
    candidates: Sequence[Mapping[str, Any]],
    scorecard: Mapping[str, Any],
) -> dict[str, Any]:
    family_rows = []
    families = sorted({family for row in candidates for family in _texts(row.get("families"))})
    for family in families:
        rows = [row for row in candidates if family in _texts(row.get("families"))]
        best = max(rows, key=lambda row: _float(row.get("overall_score"))) if rows else {}
        family_rows.append(
            {
                "family": family,
                "near_miss_count": len(rows),
                "best_variant": best.get("variant_id", ""),
                "common_failure": _common_failed_gate(rows),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    recommended = _recommended_focus_families(candidates, scorecard)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "families": family_rows,
        "recommended_focus_families": recommended,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _common_failed_gate(rows: Sequence[Mapping[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        for gate in _texts(row.get("failed_gates")):
            counts[gate] = counts.get(gate, 0) + 1
    return max(counts, key=counts.get) if counts else "none"


def _recommended_focus_families(
    candidates: Sequence[Mapping[str, Any]],
    scorecard: Mapping[str, Any],
) -> list[str]:
    families = []
    for family in ("cash_buffer", "smoothing", "candidate_ensemble", "rebalance_threshold"):
        if any(family in _texts(row.get("families")) for row in candidates):
            families.append(family)
    if not families:
        ranked = _rank_families(_records(scorecard.get("variant_scorecard")))
        families = [_text(row.get("family")) for row in ranked[:4]]
    for required in ("cash_buffer", "smoothing", "candidate_ensemble", "rebalance_threshold"):
        if required not in families:
            families.append(required)
    return families[:6]


def _scorecard_row(scorecard: Mapping[str, Any], variant_id: str) -> dict[str, Any]:
    for row in _records(scorecard.get("variant_scorecard")):
        if row.get("variant_id") == variant_id:
            return dict(row)
    return {}


def _cash_buffer_effect_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    components = _mapping(row.get("score_components"))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "variant_id": row.get("variant_id", "cash_buffer_10"),
        "family": "cash_buffer",
        "improvements": {
            "drawdown": _component_label(_float(components.get("drawdown"))),
            "turnover": _component_label(_float(components.get("turnover"))),
            "rolling_consistency": _component_label(_float(components.get("rolling_consistency"))),
            "sideways_choppy": _component_label(_float(components.get("sideways_choppy"))),
        },
        "costs": {
            "return_preservation": _cost_label(_float(components.get("return"))),
            "strong_recovery_lag": _lag_label(_float(components.get("strong_recovery_lag"))),
        },
        "overall_interpretation": _cash_buffer_interpretation(row),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _cash_buffer_failure_reason(
    row: Mapping[str, Any],
    near_miss: Mapping[str, Any],
) -> dict[str, Any]:
    failed = _failed_gates(row)
    failure = "unknown"
    if "return_preservation_gate" in failed:
        failure = "return_preservation_weak"
    elif "recovery_lag_gate" in failed:
        failure = "insufficient_robustness"
    elif "regime_gate" in failed:
        failure = "regime_mixed"
    elif "composite_score_gate" in failed:
        failure = "insufficient_robustness"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "variant_id": row.get("variant_id", "cash_buffer_10"),
        "promotion_failed": row.get("scorecard_decision") != "PROMOTE_TO_FORMAL_IMPLEMENTATION",
        "failed_gates": failed,
        "primary_failure_reason": failure,
        "can_be_refined": True,
        "recommended_refinement": [
            "cash_buffer_8",
            "cash_buffer_10_plus_smoothing_3d",
            "cash_buffer_10_plus_rebalance_threshold_3pct",
            "cash_buffer_10_plus_median_consensus",
        ],
        "near_miss_status": (
            "NEAR_MISS"
            if any(
                item.get("variant_id") == row.get("variant_id")
                for item in _records(near_miss.get("near_miss_candidates"))
            )
            else "NOT_SELECTED"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _cash_buffer_variant_recommendations(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_variant_id": row.get("variant_id", "cash_buffer_10"),
        "recommended_variants": [
            "cash_buffer_6_plus_smooth_2d",
            "cash_buffer_8_plus_smooth_3d",
            "cash_buffer_10_plus_rebalance_threshold_3pct",
            "cash_buffer_12_plus_median_consensus",
            "sideways_cooldown_5d_plus_cash_buffer_8",
        ],
        "recommended_direction": "hybrid_component_not_standalone_method",
        "reason": [
            "cash_buffer can help drawdown but may drag return",
            "targeted v3 should test smaller cash and hybrid controls",
        ],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _family_coverage_gap(
    search_space: Mapping[str, Any],
    near_miss: Mapping[str, Any],
) -> dict[str, Any]:
    covered = set(_texts(search_space.get("families")))
    near_families = set(
        _texts(
            _mapping(near_miss.get("near_miss_family_summary")).get("recommended_focus_families")
        )
    )
    gaps = [
        {
            "gap": "cash_buffer_smoothing_hybrid",
            "status": "MISSING_TARGETED_GRID",
            "reason": "near-miss cash buffer needs lower return drag and smoothing support",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "gap": "cash_buffer_threshold_hybrid",
            "status": "MISSING_TARGETED_GRID",
            "reason": "cash buffer and rebalance threshold were mostly tested separately",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "gap": "top_k_consensus_threshold",
            "status": "MISSING_TARGETED_GRID",
            "reason": "ensemble variants need threshold controls around near-miss families",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "gap": "smoothing_recovery_fast_restore",
            "status": "UNDER_COVERED",
            "reason": "smoothing can lag recovery and needs explicit restore hybrids",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "covered_families": sorted(covered),
        "near_miss_focus_families": sorted(near_families),
        "gaps": gaps,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _parameter_coverage_gap(
    search_space: Mapping[str, Any],
    attribution: Mapping[str, Any],
) -> dict[str, Any]:
    config = _mapping(search_space.get("normalized_search_space"))
    families = _mapping(config.get("families"))
    cash_values = _records_or_values(
        _mapping(families.get("cash_buffer")).get("min_cash_weight"), []
    )
    smoothing_values = _records_or_values(_mapping(families.get("smoothing")).get("windows"), [])
    gaps = [
        {
            "parameter": "cash_buffer",
            "current_values": cash_values,
            "recommended_values": [0.06, 0.08, 0.10, 0.12, 0.15],
            "reason": "cash_buffer_10 ranked high but smaller/larger grid is not fine enough",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "parameter": "smoothing_window",
            "current_values": smoothing_values,
            "recommended_values": [2, 3, 4],
            "reason": "hybrids should test shorter smoothing to reduce recovery lag",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "parameter": "rebalance_threshold",
            "current_values": [0.02, 0.03, 0.05],
            "recommended_values": [0.02, 0.025, 0.03, 0.04],
            "reason": "threshold grid needs finer low-turnover control around near misses",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "parameter": "top_k",
            "current_values": [3, 5],
            "recommended_values": [3, 5, 7],
            "reason": "top-k ensemble variants should be paired with threshold controls",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "attribution_id": attribution.get("attribution_id"),
        "gaps": gaps,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _targeted_v3_recommendations(
    family_gap: Mapping[str, Any],
    parameter_gap: Mapping[str, Any],
) -> dict[str, Any]:
    _ = (family_gap, parameter_gap)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "recommended_focus": [
            "cash_buffer_smoothing_hybrid",
            "cash_buffer_threshold_hybrid",
            "median_consensus_smoothing",
            "top5_consensus_threshold",
            "sideways_cooldown_cash_buffer",
            "smoothing_recovery_fast_restore",
        ],
        "new_parameter_ranges": {
            "cash_buffer": [0.06, 0.08, 0.10, 0.12, 0.15],
            "smoothing_window": [2, 3, 4],
            "rebalance_threshold": [0.02, 0.025, 0.03, 0.04],
            "top_k": [3, 5, 7],
        },
        "max_v3_variants": TARGETED_V3_MAX_VARIANTS,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _targeted_v3_variant_specs(
    coverage: Mapping[str, Any],
    near_miss: Mapping[str, Any],
) -> list[dict[str, Any]]:
    recommendations = _mapping(coverage.get("targeted_v3_recommendations"))
    ranges = _mapping(recommendations.get("new_parameter_ranges"))
    cash_values = [
        _float(value) for value in _records_or_values(ranges.get("cash_buffer"), [0.08, 0.10])
    ]
    windows = [
        int(_float(value))
        for value in _records_or_values(ranges.get("smoothing_window"), [2, 3, 4])
    ]
    thresholds = [
        _float(value)
        for value in _records_or_values(ranges.get("rebalance_threshold"), [0.02, 0.03])
    ]
    top_k_values = [int(_float(value)) for value in _records_or_values(ranges.get("top_k"), [3, 5])]
    near_rows = _records(near_miss.get("near_miss_candidates"))
    default_parent = (
        _text(near_rows[0].get("variant_id"), "cash_buffer_10") if near_rows else "cash_buffer_10"
    )
    variants: list[dict[str, Any]] = []
    for cash in cash_values:
        for window in windows:
            for alpha in (0.40, 0.55):
                variant_id = (
                    f"cash_buffer_{int(cash * 100)}_plus_smooth_{window}d_alpha_{int(alpha * 100)}"
                )
                variants.append(
                    _targeted_v3_variant(
                        variant_id,
                        ["cash_buffer", "smoothing"],
                        "cash_buffer_smoothing_hybrid",
                        [
                            {"type": "min_cash_weight", "min_cash_weight": cash},
                            {"type": "weight_smoothing", "window_days": window, "alpha": alpha},
                        ],
                        default_parent,
                        "cash_buffer_smoothing_hybrid_gap",
                    )
                )
    for cash in cash_values:
        for threshold in thresholds:
            variants.append(
                _targeted_v3_variant(
                    f"cash_buffer_{int(cash * 100)}_plus_rebalance_delta_{int(threshold * 1000)}bp",
                    ["cash_buffer", "rebalance_threshold"],
                    "cash_buffer_threshold_hybrid",
                    [
                        {"type": "min_cash_weight", "min_cash_weight": cash},
                        {"type": "rebalance_threshold", "min_total_abs_delta": threshold},
                    ],
                    default_parent,
                    "cash_buffer_threshold_hybrid_gap",
                )
            )
    for method in ("median", "trimmed_mean", "weighted_mean"):
        for window in windows:
            variants.append(
                _targeted_v3_variant(
                    f"{method}_consensus_plus_smooth_{window}d",
                    ["candidate_ensemble", "smoothing"],
                    "median_consensus_smoothing",
                    [
                        {"type": "consensus_aggregation", "method": method},
                        {"type": "weight_smoothing", "window_days": window, "alpha": 0.50},
                    ],
                    default_parent,
                    "ensemble_smoothing_gap",
                )
            )
    for top_k in top_k_values:
        for threshold in thresholds:
            variants.append(
                _targeted_v3_variant(
                    f"top{top_k}_consensus_plus_threshold_{int(threshold * 1000)}bp",
                    ["candidate_ensemble", "rebalance_threshold"],
                    "top_k_consensus_threshold",
                    [
                        {"type": "candidate_subset", "top_k": top_k},
                        {"type": "consensus_aggregation", "method": "weighted_mean"},
                        {"type": "rebalance_threshold", "min_total_abs_delta": threshold},
                    ],
                    default_parent,
                    "top_k_threshold_gap",
                )
            )
    for cooldown_days in (3, 5):
        for cash in cash_values:
            variants.append(
                _targeted_v3_variant(
                    f"sideways_cooldown_{cooldown_days}d_plus_cash_buffer_{int(cash * 100)}",
                    ["cooldown", "cash_buffer"],
                    "sideways_cooldown_cash_buffer",
                    [
                        {
                            "type": "regime_cooldown",
                            "regime": "sideways_choppy",
                            "cooldown_days": cooldown_days,
                        },
                        {"type": "min_cash_weight", "min_cash_weight": cash},
                    ],
                    default_parent,
                    "sideways_cash_buffer_gap",
                )
            )
    for window in windows:
        for multiplier in (0.85, 0.95):
            variants.append(
                _targeted_v3_variant(
                    f"smooth_{window}d_plus_strong_recovery_restore_{int(multiplier * 100)}",
                    ["smoothing", "regime_gating"],
                    "smoothing_recovery_fast_restore",
                    [
                        {"type": "weight_smoothing", "window_days": window, "alpha": 0.50},
                        {
                            "type": "regime_gate",
                            "regime": "strong_recovery",
                            "action": "reduce_active_tilt",
                            "multiplier": multiplier,
                        },
                    ],
                    default_parent,
                    "recovery_fast_restore_gap",
                )
            )
    return _dedupe_variants(variants)


def _targeted_v3_variant(
    variant_id: str,
    families: Sequence[str],
    targeted_family: str,
    transforms: Sequence[Mapping[str, Any]],
    near_miss_parent: str,
    gap_reason: str,
) -> dict[str, Any]:
    variant = _variant(
        variant_id,
        families,
        transforms,
        ["return_preservation_weak", "rolling_consistency_unstable", "turnover_high"],
        ["target near-miss weakness with narrower hybrid search"],
        ["may still fail promotion or recovery confirmation"],
    )
    variant.update(
        {
            "targeted_family": targeted_family,
            "near_miss_parent": near_miss_parent,
            "coverage_gap_reason": gap_reason,
            "expected_benefit": [
                "retain near-miss benefit while reducing the most common failed gate"
            ],
            "expected_cost": ["may still lag strong recovery or dilute return"],
        }
    )
    return variant


def _targeted_v3_family_coverage(variants: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    targeted = sorted(
        {_text(row.get("targeted_family")) for row in variants if row.get("targeted_family")}
    )
    by_targeted = {
        family: sum(1 for row in variants if row.get("targeted_family") == family)
        for family in targeted
    }
    base_coverage = _batch2_family_coverage(variants)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "targeted_families_covered": targeted,
        "targeted_family_counts": by_targeted,
        "base_family_coverage": base_coverage,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _targeted_v3_scorecard_rows(
    backfill: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> list[dict[str, Any]]:
    payload = {
        "data_quality_status": backfill.get("data_quality_status"),
        "variant_performance_metrics": backfill.get("v3_variant_performance"),
        "variant_stability_metrics": backfill.get("v3_variant_stability_metrics"),
        "variant_churn_metrics": backfill.get("v3_variant_churn_metrics"),
        "variant_lag_metrics": backfill.get("v3_variant_lag_metrics"),
        "variant_regime_metrics": backfill.get("v3_variant_regime_metrics"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return _scorecard_rows(payload, _records(matrix.get("v3_variant_specs")))


def _ab_comparison_rows(
    backfill: Mapping[str, Any],
    matrix: Mapping[str, Any],
    source_scorecard: Mapping[str, Any],
) -> list[dict[str, Any]]:
    score_rows = _targeted_v3_scorecard_rows(backfill, matrix)
    source_rows = {
        _text(row.get("variant_id")): row
        for row in _records(source_scorecard.get("variant_scorecard"))
    }
    specs = {_text(row.get("variant_id")): row for row in _records(matrix.get("v3_variant_specs"))}
    rows = []
    for row in score_rows:
        spec = _mapping(specs.get(_text(row.get("variant_id"))))
        parent_id = _text(spec.get("near_miss_parent"), "cash_buffer_10")
        parent = _mapping(source_rows.get(parent_id))
        smooth = _mapping(
            source_rows.get("smooth_weights_3d")
            or source_rows.get("smooth_3d_plus_rebalance_delta_3pct")
        )
        rows.append(
            {
                "variant_id": row.get("variant_id"),
                "near_miss_parent": parent_id,
                "overall_score": row.get("overall_score"),
                "parent_overall_score": parent.get("overall_score", 0.0),
                "smooth_reference_score": smooth.get("overall_score", 0.0),
                "score_delta_vs_parent": round(
                    _float(row.get("overall_score")) - _float(parent.get("overall_score")), 6
                ),
                "return_delta_vs_parent": round(
                    _float(row.get("total_return")) - _float(parent.get("total_return")), 10
                ),
                "drawdown_delta_vs_parent": round(
                    _float(row.get("max_drawdown")) - _float(parent.get("max_drawdown")), 10
                ),
                "turnover_delta_vs_parent": round(
                    _float(row.get("turnover")) - _float(parent.get("turnover")), 10
                ),
                "ab_status": _ab_status(row, parent),
                "failed_gates": _failed_gates(row),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(rows, key=lambda item: _float(item.get("overall_score")), reverse=True)


def _ab_status(row: Mapping[str, Any], parent: Mapping[str, Any]) -> str:
    if not parent:
        return "V3_REVIEW_REQUIRED"
    score_delta = _float(row.get("overall_score")) - _float(parent.get("overall_score"))
    return_delta = _float(row.get("total_return")) - _float(parent.get("total_return"))
    drawdown_delta = _float(row.get("max_drawdown")) - _float(parent.get("max_drawdown"))
    if score_delta > 0 and drawdown_delta >= 0 and return_delta >= -0.005:
        return "V3_WINS"
    if score_delta < -0.03 and return_delta < 0 and drawdown_delta < 0:
        return "PARENT_WINS"
    return "MIXED"


def _ab_winner_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best = max(rows, key=lambda row: _float(row.get("overall_score"))) if rows else {}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "best_v3_variant": best.get("variant_id", ""),
        "best_v3_score": best.get("overall_score", 0.0),
        "v3_win_count": sum(1 for row in rows if row.get("ab_status") == "V3_WINS"),
        "parent_win_count": sum(1 for row in rows if row.get("ab_status") == "PARENT_WINS"),
        "inconclusive_count": sum(
            1 for row in rows if row.get("ab_status") in {"MIXED", "V3_REVIEW_REQUIRED"}
        ),
        "recommended_next_action": "promotion_threshold_sensitivity",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _threshold_scenarios(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    scenario_specs = [
        (
            "base_threshold",
            BATCH2_PROMOTE_SCORE,
            True,
            "Base promotion gate remains authoritative.",
        ),
        (
            "slightly_relaxed_return_preservation",
            BATCH2_PROMOTE_SCORE - NO_PROMOTION_NEAR_MISS_MARGIN,
            False,
            "For diagnostics only; do not auto-promote under relaxed thresholds.",
        ),
        (
            "slightly_relaxed_composite_score",
            BATCH2_PROMOTE_SCORE - 0.03,
            False,
            "For diagnostics only; score-only relaxation requires owner review.",
        ),
    ]
    scenarios = []
    for name, threshold, recommended, reason in scenario_specs:
        promoted = [
            row
            for row in rows
            if _float(row.get("overall_score")) >= threshold and not _high_risk_gate_failure(row)
        ]
        scenarios.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "scenario": name,
                "score_threshold": round(threshold, 6),
                "promote_count": len(promoted),
                "high_risk_promote_count": sum(
                    1 for row in promoted if _high_risk_gate_failure(row)
                ),
                "recommended": recommended,
                "reason": reason,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return scenarios


def _threshold_candidate_impact(
    rows: Sequence[Mapping[str, Any]],
    ab: Mapping[str, Any],
    scenarios: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    _ = scenarios
    ab_rows = {
        _text(row.get("variant_id")): row for row in _records(ab.get("ab_comparison_matrix"))
    }
    base = {
        _text(row.get("variant_id"))
        for row in rows
        if row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    }
    relaxed = [
        row
        for row in rows
        if _float(row.get("overall_score")) >= BATCH2_PROMOTE_SCORE - NO_PROMOTION_NEAR_MISS_MARGIN
        and not _high_risk_gate_failure(row)
        and _text(row.get("variant_id")) not in base
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "base_promoted_candidates": sorted(base),
        "relaxed_only_candidates": [
            {
                "variant_id": row.get("variant_id"),
                "overall_score": row.get("overall_score"),
                "failed_gates": _failed_gates(row),
                "ab_status": _mapping(ab_rows.get(_text(row.get("variant_id")))).get(
                    "ab_status", "MIXED"
                ),
                "candidate_status": "REVIEW_REQUIRED",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for row in relaxed[:20]
        ],
        "policy_effect": "diagnostic_only_no_gate_change",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _promotion_v2_candidate_lists(
    rows: Sequence[Mapping[str, Any]],
    ab: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    ab_rows = {
        _text(row.get("variant_id")): row for row in _records(ab.get("ab_comparison_matrix"))
    }
    relaxed = {
        _text(row.get("variant_id"))
        for row in _records(
            _mapping(sensitivity.get("threshold_candidate_impact")).get("relaxed_only_candidates")
        )
    }
    promoted: list[dict[str, Any]] = []
    keep: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in rows:
        variant_id = _text(row.get("variant_id"))
        ab_status = _text(_mapping(ab_rows.get(variant_id)).get("ab_status"), "MIXED")
        payload = {
            "variant_id": variant_id,
            "overall_score": row.get("overall_score"),
            "scorecard_decision": row.get("scorecard_decision"),
            "failed_gates": _failed_gates(row),
            "ab_status": ab_status,
            "candidate_status": "",
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        if (
            row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
            and ab_status in {"V3_WINS", "MIXED", "V3_REVIEW_REQUIRED"}
            and not _high_risk_gate_failure(row)
        ):
            payload["candidate_status"] = "PROMOTED_V2"
            promoted.append(payload)
        elif row.get("scorecard_decision") == "KEEP_FOR_MORE_TESTING" or variant_id in relaxed:
            payload["candidate_status"] = "KEEP_TESTING"
            keep.append(payload)
        else:
            payload["candidate_status"] = "REJECTED_V2"
            rejected.append(payload)
    return promoted[:3], keep[:20], rejected


def _promotion_v2_decision(
    promoted: Sequence[Mapping[str, Any]],
    keep: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if promoted:
        decision = "PROMOTE_CANDIDATE"
        next_action = "formal_method_auto_plan"
    elif keep:
        decision = "KEEP_TESTING"
        next_action = "continue_targeted_search"
    elif rejected:
        decision = "RUN_ANOTHER_TARGETED_SEARCH"
        next_action = "continue_targeted_search"
    else:
        decision = "NO_CANDIDATE"
        next_action = "owner_review"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "promotion_v2_id": "",
        "decision": decision,
        "promoted_count": len(promoted),
        "keep_testing_count": len(keep),
        "rejected_count": len(rejected),
        "recommended_next_action": next_action,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _next_plan_decision(promotion: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(promotion.get("promotion_v2_decision"))
    promotion_decision = _text(decision.get("decision"))
    if promotion_decision == "PROMOTE_CANDIDATE":
        plan_decision = "FORMAL_METHOD_PLAN"
        next_action = "draft_research_only_formal_method_plan"
        continue_search = False
    elif promotion_decision == "KEEP_TESTING":
        plan_decision = "KEEP_TESTING_PLAN"
        next_action = "continue_paper_shadow_observation"
        continue_search = True
    elif promotion_decision == "RUN_ANOTHER_TARGETED_SEARCH":
        plan_decision = "CONTINUE_SEARCH_PLAN"
        next_action = "run_smaller_v4_or_signal_level_diagnosis"
        continue_search = True
    else:
        plan_decision = "NO_CANDIDATE_PLAN"
        next_action = "return_to_signal_level_diagnosis"
        continue_search = False
    return {
        "schema_version": st.SCHEMA_VERSION,
        "plan_id": "",
        "decision": plan_decision,
        "source_promotion_v2_decision": promotion_decision,
        "recommended_next_action": next_action,
        "should_continue_parameter_search": continue_search,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _next_formal_method_candidates(promotion: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in _records(promotion.get("promoted_candidates_v2")):
        rows.append(
            {
                "variant_id": row.get("variant_id"),
                "implementation_scope": "research_only",
                "transform_composable": True,
                "requires_external_data": False,
                "implementation_complexity": "MEDIUM",
                "implementation_allowed_without_owner_approval": False,
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                "production_effect": st.PRODUCTION_EFFECT,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": st.SCHEMA_VERSION, "candidates": rows, **st.EXPERIMENT_FACTORY_SAFETY}


def _continue_search_plan(
    promotion: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    promotion_decision = _text(_mapping(promotion.get("promotion_v2_decision")).get("decision"))
    if promotion_decision == "KEEP_TESTING":
        actions = [
            "continue paper shadow observation",
            "collect forward confirmation",
            "run small v4 around keep-testing candidates only",
        ]
        priority = "targeted_observation"
    elif promotion_decision == "RUN_ANOTHER_TARGETED_SEARCH":
        actions = [
            "do not blindly expand parameters",
            "return to signal-level diagnosis",
            "only run v4 if a specific gate failure hypothesis is documented",
        ]
        priority = "signal_level_diagnosis"
    elif promotion_decision == "PROMOTE_CANDIDATE":
        actions = [
            "prepare formal method implementation plan",
            "run owner review before implementation",
        ]
        priority = "formal_plan"
    else:
        actions = [
            "keep smooth_weights_3d as primary observation candidate",
            "lower batch search priority",
            "return to feature and signal diagnosis",
        ]
        priority = "stop_parameter_expansion"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "recommended_actions": actions,
        "priority": priority,
        "should_continue_parameter_search": decision.get("should_continue_parameter_search"),
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _component_label(score: float) -> str:
    if score >= 0.65:
        return "IMPROVED"
    if score >= 0.40:
        return "MIXED"
    if score > 0:
        return "WORSE"
    return "INSUFFICIENT_DATA"


def _cost_label(score: float) -> str:
    if score >= 0.70:
        return "GOOD"
    if score >= 0.45:
        return "ACCEPTABLE"
    if score > 0:
        return "POOR"
    return "INSUFFICIENT_DATA"


def _lag_label(score: float) -> str:
    if score >= 0.70:
        return "LOW"
    if score >= 0.35:
        return "MEDIUM"
    if score > 0:
        return "HIGH"
    return "INSUFFICIENT_DATA"


def _cash_buffer_interpretation(row: Mapping[str, Any]) -> str:
    components = _mapping(row.get("score_components"))
    if _float(components.get("drawdown")) >= 0.60 and _float(components.get("return")) < 0.45:
        return "defensive_buffer_helped_but_return_cost_too_high"
    if _float(components.get("drawdown")) >= 0.60:
        return "defensive_buffer_helped_but_needs_confirmation"
    return "cash_buffer_effect_mixed"


def _reason_for_gate(gate: str) -> str:
    return {
        "composite_score_gate": "composite_score_below_promotion_threshold",
        "return_preservation_gate": "return_preservation_weak",
        "drawdown_gate": "insufficient_drawdown_improvement",
        "rolling_consistency_gate": "rolling_consistency_not_strong_enough",
        "turnover_gate": "turnover_not_low_enough",
        "regime_gate": "regime_behavior_mixed",
        "recovery_lag_gate": "strong_recovery_lag_too_high",
        "data_quality_gate": "data_quality_warning_or_failure",
    }.get(gate, gate)


def _gate_failure_severity(gate: str, count: int, total: int) -> str:
    if gate in {"drawdown_gate", "regime_gate", "data_quality_gate", "recovery_lag_gate"}:
        return "HIGH"
    if total and count / total >= 0.50:
        return "HIGH"
    return "MEDIUM"


def _high_risk_gate_failure(row: Mapping[str, Any]) -> bool:
    return bool(
        {"drawdown_gate", "regime_gate", "data_quality_gate", "recovery_lag_gate"}
        & set(_failed_gates(row))
    )


def _percentile(values: Sequence[float], percentile: float) -> float:
    clean = sorted(values)
    if not clean:
        return 0.0
    idx = min(len(clean) - 1, max(0, int(round((len(clean) - 1) * percentile))))
    return clean[idx]


def _search_family_inventory(config: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for family, payload in sorted(_mapping(config.get("families")).items()):
        data = _mapping(payload)
        params = [
            key
            for key, value in data.items()
            if key != "enabled" and isinstance(value, (list, tuple))
        ]
        rows.append(
            {
                "family": family,
                "enabled": data.get("enabled") is True,
                "parameters": params,
                "parameter_count": sum(
                    len(_records_or_values(data.get(key), [])) for key in params
                ),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": st.SCHEMA_VERSION, "families": rows, **st.EXPERIMENT_FACTORY_SAFETY}


def _batch2_family_coverage(variants: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    families = sorted({family for row in variants for family in _texts(row.get("families"))})
    failure_modes = sorted(
        {mode for row in variants for mode in _texts(row.get("target_failure_modes"))}
    )
    by_family = {
        family: sum(1 for row in variants if family in _texts(row.get("families")))
        for family in families
    }
    by_failure = {
        mode: sum(1 for row in variants if mode in _texts(row.get("target_failure_modes")))
        for mode in failure_modes
    }
    return {
        "schema_version": st.SCHEMA_VERSION,
        "families_covered": families,
        "family_counts": by_family,
        "failure_modes_covered": failure_modes,
        "failure_mode_counts": by_failure,
        "coverage_status": "PASS" if len(families) >= 8 else "FAIL",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _variant_churn_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    stability_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    stability = {str(row.get("variant_id")): row for row in stability_rows}
    rows = []
    for variant_id in sorted({str(row.get("variant_id")) for row in variant_states}):
        states = [row for row in variant_states if row.get("variant_id") == variant_id]
        turnover_values = [
            _float(row.get("turnover")) for row in states if row.get("rebalance_event") is True
        ]
        stable = _mapping(stability.get(variant_id))
        rows.append(
            {
                "variant_id": variant_id,
                "avg_rebalance_turnover": (
                    round(sum(turnover_values) / len(turnover_values), 10)
                    if turnover_values
                    else 0.0
                ),
                "max_rebalance_turnover": (
                    round(max(turnover_values), 10) if turnover_values else 0.0
                ),
                "signal_churn_count": int(_float(stable.get("weight_flip_count"))),
                "large_jump_count": int(_float(stable.get("large_jump_count"))),
                "churn_status": (
                    "LOW" if _float(stable.get("large_jump_count")) <= 1 else "REVIEW_REQUIRED"
                ),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _variant_lag_metrics(regime_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    by_variant = sorted({str(row.get("variant_id")) for row in regime_rows})
    for variant_id in by_variant:
        recovery = [
            row
            for row in regime_rows
            if row.get("variant_id") == variant_id and row.get("regime") == "strong_recovery"
        ]
        delta = _float(recovery[0].get("relative_to_limited_adjustment")) if recovery else 0.0
        if not recovery or recovery[0].get("regime_status") == "INSUFFICIENT_DATA":
            status = "INSUFFICIENT_DATA"
        elif delta <= BATCH2_STRONG_RECOVERY_HIGH_LAG_DELTA:
            status = "HIGH"
        elif delta < 0:
            status = "MEDIUM"
        else:
            status = "LOW"
        rows.append(
            {
                "variant_id": variant_id,
                "strong_recovery_return_delta_vs_limited": round(delta, 10),
                "lag_cost_status": status,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _rolling_robustness_rows(
    scorecard: Mapping[str, Any],
    backfill: Mapping[str, Any],
    top_ids: Sequence[str],
) -> list[dict[str, Any]]:
    states = _records(backfill.get("variant_weight_paths"))
    rows = []
    for variant_id in top_ids:
        selected = [row for row in states if row.get("variant_id") == variant_id]
        windows = st._rolling_window_inventory(
            selected, min_observations=st.DEFAULT_MIN_EVAL_OBSERVATIONS
        )
        pass_count = 0
        fail_count = 0
        for window in windows:
            metrics = st._state_path_metrics(
                [
                    row
                    for row in selected
                    if _coerce_date(window.get("start_date"), date(1970, 1, 1))
                    <= _coerce_date(row.get("date"), date(1970, 1, 1))
                    <= _coerce_date(window.get("end_date"), date(1970, 1, 1))
                ],
                min_observations=2,
            )
            if metrics.get("status") == "INSUFFICIENT_DATA":
                continue
            if _float(metrics.get("max_drawdown")) >= -0.20:
                pass_count += 1
            else:
                fail_count += 1
        rows.append(
            {
                "variant_id": variant_id,
                "window_count": len(windows),
                "rolling_pass_count": pass_count,
                "rolling_fail_count": fail_count,
                "rolling_status": "ROBUST" if pass_count >= fail_count else "WEAK",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _robustness_summary(
    top_ids: Sequence[str],
    rolling: Sequence[Mapping[str, Any]],
    regime: Sequence[Mapping[str, Any]],
    stability: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rolling_status = {str(row.get("variant_id")): row for row in rolling}
    stability_status = {str(row.get("variant_id")): row for row in stability}
    robust = []
    weak = []
    for variant_id in top_ids:
        regime_worse = [
            row
            for row in regime
            if row.get("variant_id") == variant_id and row.get("regime_status") == "WORSE"
        ]
        stable = _mapping(stability_status.get(variant_id))
        rolling_row = _mapping(rolling_status.get(variant_id))
        if (
            len(regime_worse) <= 1
            and stable.get("stability_status") in {"STABLE", "MODERATE"}
            and rolling_row.get("rolling_status") != "WEAK"
        ):
            robust.append(variant_id)
        else:
            weak.append(variant_id)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "robust_candidates": robust,
        "weak_candidates": weak,
        "recommended_next_action": "promotion_gate" if robust else "expanded_search",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _pareto_frontier(scorecard: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    candidates = []
    for row in scorecard:
        dominated = False
        for other in scorecard:
            if row is other:
                continue
            better_or_equal = (
                _float(other.get("total_return")) >= _float(row.get("total_return"))
                and _float(other.get("max_drawdown")) >= _float(row.get("max_drawdown"))
                and _float(other.get("turnover")) <= _float(row.get("turnover"))
            )
            strictly_better = (
                _float(other.get("total_return")) > _float(row.get("total_return"))
                or _float(other.get("max_drawdown")) > _float(row.get("max_drawdown"))
                or _float(other.get("turnover")) < _float(row.get("turnover"))
            )
            if better_or_equal and strictly_better:
                dominated = True
                break
        if not dominated:
            candidates.append(_text(row.get("variant_id")))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidates": candidates[:20],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _score_distribution(scorecard: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    scores = sorted(_float(row.get("overall_score")) for row in scorecard)
    decisions = [_text(row.get("scorecard_decision")) for row in scorecard]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "variant_count": len(scorecard),
        "min_score": scores[0] if scores else 0.0,
        "median_score": scores[len(scores) // 2] if scores else 0.0,
        "max_score": scores[-1] if scores else 0.0,
        "promote_count": decisions.count("PROMOTE_TO_FORMAL_IMPLEMENTATION"),
        "keep_testing_count": decisions.count("KEEP_FOR_MORE_TESTING"),
        "reject_count": decisions.count("REJECT"),
        "defer_count": decisions.count("DEFER_FOR_FORWARD_DATA"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _batch2_matrix_payload(*, matrix_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / matrix_id
    if not (root / "batch2_matrix_manifest.json").exists():
        raise RuntimeError(f"batch2 matrix artifact not found: {root}")
    return {
        **_read_json(root / "batch2_matrix_manifest.json"),
        "variant_specs": _read_jsonl(root / "batch2_variant_specs.jsonl"),
        "family_coverage": _read_json(root / "batch2_family_coverage.json"),
        "matrix_dir": str(root),
    }


def _latest_common_price_date(pivot: pd.DataFrame, symbols: Sequence[str]) -> date:
    if pivot.empty:
        raise RuntimeError("price cache has no rows for batch backfill symbols")
    latest_dates = []
    for symbol in symbols:
        if symbol in pivot.columns:
            series = pivot[symbol].dropna()
            if not series.empty:
                latest_dates.append(series.index[-1].date())
    if not latest_dates:
        raise RuntimeError("price cache has no complete symbol coverage")
    return min(latest_dates)


def _promotion_decision_summary(decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "promoted_count": sum(
            1 for row in decisions if row.get("decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
        ),
        "keep_testing_count": sum(
            1 for row in decisions if row.get("decision") == "KEEP_FOR_MORE_TESTING"
        ),
        "rejected_count": sum(1 for row in decisions if row.get("decision") == "REJECT"),
        "defer_count": sum(
            1 for row in decisions if row.get("decision") == "DEFER_FOR_FORWARD_DATA"
        ),
    }


def _promoted_candidate_spec(row: Mapping[str, Any]) -> dict[str, Any]:
    variant_id = _text(row.get("variant_id"))
    return {
        "variant_id": variant_id,
        "proposed_method_name": f"{variant_id}_limited_adjustment_research_method",
        "families": _texts(row.get("families")),
        "implementation_complexity": row.get("implementation_complexity", "MEDIUM"),
        "transform_composable": True,
        "requires_external_data": False,
        "implementation_scope": "research_only",
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _failure_mode_coverage_from_explanations(
    explanations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    families = {family for row in explanations for family in _texts(row.get("families"))}
    rows = [
        {
            "failure_mode": mode,
            "coverage_status": "COVERED" if families else "MISSING",
            "covered_by_families": sorted(families),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for mode in st.DEFAULT_FAILURE_MODES
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "failure_modes": rows,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _top_by(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    if not rows:
        return ""
    return _text(max(rows, key=lambda row: _float(row.get(key))).get("variant_id"))


def _top_stability(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return ""
    return _text(
        max(
            rows,
            key=lambda row: (
                _label_score(
                    _text(row.get("rolling_consistency_delta")),
                    {"IMPROVED": 2.0, "MIXED": 1.0, "INSUFFICIENT_DATA": 0.0, "WORSE": -1.0},
                ),
                _float(row.get("overall_score")),
            ),
        ).get("variant_id")
    )


def _rank_families(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    scores: dict[str, list[float]] = {}
    for row in rows:
        for family in _texts(row.get("families")):
            scores.setdefault(family, []).append(_float(row.get("overall_score")))
    return [
        {
            "family": family,
            "avg_score": round(sum(values) / len(values), 6),
            "candidate_count": len(values),
        }
        for family, values in sorted(
            scores.items(), key=lambda item: sum(item[1]) / len(item[1]), reverse=True
        )
    ]


def _top_reject_reasons(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        for flag in _texts(row.get("hard_reject_flags")):
            counts[flag] = counts.get(flag, 0) + 1
    return [
        {"reason": reason, "count": count}
        for reason, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:8]
    ]


def _scorecard_reason(
    decision: str,
    flags: Sequence[str],
    perf: Mapping[str, Any],
    stable: Mapping[str, Any],
    lag: Mapping[str, Any],
) -> list[str]:
    if flags:
        return [*flags, "hard_reject_rule_applied"]
    return [
        f"decision={decision}",
        f"return_delta={perf.get('relative_to_limited_adjustment')}",
        f"drawdown_delta={perf.get('drawdown_delta_vs_limited')}",
        f"rolling={stable.get('rolling_consistency_delta')}",
        f"lag={lag.get('lag_cost_status')}",
    ]


def _risk_adjusted(perf: Mapping[str, Any]) -> float:
    vol = _float(perf.get("realized_volatility"))
    if vol <= 0:
        return 0.0
    return _float(perf.get("annualized_return")) / vol


def _regime_component(regime_rows: Sequence[Mapping[str, Any]], regime: str) -> float:
    row = next((item for item in regime_rows if item.get("regime") == regime), {})
    return _label_score(
        _text(_mapping(row).get("regime_status")),
        {"IMPROVED": 1.0, "MIXED": 0.55, "INSUFFICIENT_DATA": 0.2, "WORSE": 0.0},
    )


def _simplicity_score(spec: Mapping[str, Any]) -> float:
    count = len(_records(spec.get("transforms")))
    if count <= 1:
        return 1.0
    if count == 2:
        return 0.75
    return 0.45


def _family_benefits(families: Sequence[str]) -> list[str]:
    mapping = {
        "smoothing": "reduces weight jumps",
        "cooldown": "reduces sideways signal churn",
        "regime_gating": "targets pressure-regime behavior",
        "candidate_ensemble": "reduces single-candidate noise",
        "rebalance_threshold": "lowers small rebalances",
        "cash_buffer": "adds drawdown cushion",
        "risk_exposure_control": "caps concentrated exposure",
        "turnover_control": "caps rebalance turnover",
    }
    return [mapping.get(family, f"tests {family}") for family in families]


def _family_costs(families: Sequence[str]) -> list[str]:
    costs = []
    if "smoothing" in families or "cooldown" in families:
        costs.append("may lag fast recovery")
    if "cash_buffer" in families or "risk_exposure_control" in families:
        costs.append("may sacrifice upside")
    if "candidate_ensemble" in families:
        costs.append("may dilute the strongest candidate")
    return costs or ["requires forward confirmation"]


def _bounded_score(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _label_score(label: str, mapping: Mapping[str, float]) -> float:
    return _float(mapping.get(label), 0.0)


def _consensus_method(method: str) -> str:
    if method in {"median", "median_target_weights"}:
        return "median"
    if method in {"trimmed_mean", "trimmed_mean_target_weights"}:
        return "trimmed_mean"
    return "weighted_mean"


def _enabled_families(config: Mapping[str, Any]) -> list[str]:
    return [
        family
        for family, payload in sorted(_mapping(config.get("families")).items())
        if _mapping(payload).get("enabled") is True
    ]


def _assert_weight_search_safety(safety: Mapping[str, Any]) -> None:
    if not _weight_search_safety_locked(safety):
        raise ValueError("weight search safety boundary is not locked")


def _weight_search_safety_locked(safety: Mapping[str, Any]) -> bool:
    return (
        safety.get("research_screening_only") is True
        and safety.get("experiment_only") is True
        and safety.get("not_formal_research_method") is True
        and safety.get("not_official_target_weights") is True
        and safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("production_effect") == st.PRODUCTION_EFFECT
        and safety.get("auto_apply") is False
    )


def _dedupe_variants(variants: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    rows = []
    for variant in variants:
        variant_id = _text(variant.get("variant_id"))
        if variant_id in seen:
            continue
        seen.add(variant_id)
        rows.append(dict(variant))
    return rows


def _records_or_values(value: Any, default: Sequence[Any]) -> list[Any]:
    if isinstance(value, list | tuple):
        return list(value)
    return list(default)


_mapping = st._mapping
_records = st._records
_texts = st._texts
_text = st._text
_float = st._float
_coerce_date = st._coerce_date
_stable_id = st._stable_id
_unique_dir = st._unique_dir
_write_json = st._write_json
_write_jsonl = st._write_jsonl
_write_text = st._write_text
_read_json = st._read_json
_read_jsonl = st._read_jsonl
_read_optional_json = st._read_optional_json
_write_latest_pointer = st._write_latest_pointer
_artifact_dir = st._artifact_dir
_required_file_checks = st._required_file_checks
_validation_payload = st._validation_payload
_payload_safe = st._payload_safe
_payload_experiment_safe = st._payload_experiment_safe
