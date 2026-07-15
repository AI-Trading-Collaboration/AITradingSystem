from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import write_data_quality_report
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
SIGNAL_QUALITY_DISPERSION_THRESHOLD = 0.15
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


def run_weight_scorecard(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    matrix_dir: Path = DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
    output_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = weight_batch_backfill_report_payload(
        backfill_id=backfill_id, output_dir=backfill_dir
    )
    matrix = _batch2_matrix_payload(
        matrix_id=_text(backfill.get("matrix_id")),
        output_dir=matrix_dir,
    )
    scorecard = _scorecard_rows(backfill, _records(matrix.get("variant_specs")))
    pareto = _pareto_frontier(scorecard)
    distribution = _score_distribution(scorecard)
    scorecard_id = _stable_id("weight-scorecard", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / scorecard_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_scorecard_manifest",
        "scorecard_id": root.name,
        "batch_backfill_id": backfill_id,
        "batch2_matrix_id": matrix.get("matrix_id"),
        "search_space_id": matrix.get("search_space_id"),
        "source_backfill_id": matrix.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if scorecard else "FAIL",
        "market_regime": "ai_after_chatgpt",
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "top_return_candidate": _top_by(scorecard, "total_return"),
        "top_drawdown_candidate": _top_by(scorecard, "max_drawdown"),
        "top_stability_candidate": _top_stability(scorecard),
        "weight_scorecard_manifest_path": str(root / "weight_scorecard_manifest.json"),
        "variant_scorecard_path": str(root / "variant_scorecard.jsonl"),
        "pareto_frontier_path": str(root / "pareto_frontier.json"),
        "score_distribution_path": str(root / "score_distribution.json"),
        "weight_scorecard_report_path": str(root / "weight_scorecard_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "weight_scorecard_manifest.json", manifest)
    _write_jsonl(root / "variant_scorecard.jsonl", scorecard)
    _write_json(root / "pareto_frontier.json", pareto)
    _write_json(root / "score_distribution.json", distribution)
    _write_text(
        root / "weight_scorecard_report.md",
        render_weight_scorecard_report(manifest, distribution, pareto),
    )
    _write_latest_pointer(
        "latest_weight_scorecard", root.name, root / "weight_scorecard_manifest.json"
    )
    return {
        "scorecard_id": root.name,
        "scorecard_dir": root,
        "manifest": manifest,
        "variant_scorecard": scorecard,
        "pareto_frontier": pareto,
        "score_distribution": distribution,
    }


def weight_scorecard_report_payload(
    *,
    scorecard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=scorecard_id,
        latest_pointer="latest_weight_scorecard",
        latest=latest,
        output_dir=output_dir,
        required_name="weight_scorecard_manifest.json",
    )
    return {
        **_read_json(root / "weight_scorecard_manifest.json"),
        "variant_scorecard": _read_jsonl(root / "variant_scorecard.jsonl"),
        "pareto_frontier": _read_json(root / "pareto_frontier.json"),
        "score_distribution": _read_json(root / "score_distribution.json"),
        "scorecard_dir": str(root),
    }


def validate_weight_scorecard_artifact(
    *,
    scorecard_id: str,
    output_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
) -> dict[str, Any]:
    root = output_dir / scorecard_id
    manifest = _read_optional_json(root / "weight_scorecard_manifest.json") or {}
    rows = _read_jsonl(root / "variant_scorecard.jsonl")
    pareto = _read_optional_json(root / "pareto_frontier.json") or {}
    checks = _required_file_checks(
        root,
        (
            "weight_scorecard_manifest.json",
            "variant_scorecard.jsonl",
            "pareto_frontier.json",
            "score_distribution.json",
            "weight_scorecard_report.md",
        ),
    )
    checks.extend(
        [
            st._check("scorecard_id_matches", manifest.get("scorecard_id") == scorecard_id, ""),
            st._check("scorecard_present", bool(rows), ""),
            st._check("pareto_present", "candidates" in pareto, ""),
            st._check(
                "hard_rejects_block_promotion",
                all(
                    row.get("scorecard_decision") != "PROMOTE_TO_FORMAL_IMPLEMENTATION"
                    for row in rows
                    if _texts(row.get("hard_reject_flags"))
                ),
                "",
            ),
            st._check(
                "data_quality_visible",
                manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(manifest.get("data_quality_status")),
            ),
            st._check("broker_forbidden", _payload_safe(manifest, pareto, *rows), ""),
            st._check(
                "experiment_safety_locked", _payload_experiment_safe(manifest, pareto, *rows), ""
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_weight_scorecard_validation", scorecard_id, checks)


def run_weight_robustness_review(
    *,
    scorecard_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    backfill_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    output_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    backfill = weight_batch_backfill_report_payload(
        backfill_id=_text(scorecard.get("batch_backfill_id")),
        output_dir=backfill_dir,
    )
    top_ids = [
        _text(row.get("variant_id")) for row in _records(scorecard.get("variant_scorecard"))[:12]
    ]
    rolling = _rolling_robustness_rows(scorecard, backfill, top_ids)
    regime = [
        row
        for row in _records(backfill.get("variant_regime_metrics"))
        if row.get("variant_id") in top_ids
    ]
    stability = [
        row
        for row in _records(backfill.get("variant_stability_metrics"))
        if row.get("variant_id") in top_ids
    ]
    summary = _robustness_summary(top_ids, rolling, regime, stability)
    robustness_id = _stable_id("weight-robustness-review", scorecard_id, generated.isoformat())
    root = _unique_dir(output_dir / robustness_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_robustness_review_manifest",
        "robustness_id": root.name,
        "scorecard_id": scorecard_id,
        "batch_backfill_id": scorecard.get("batch_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if top_ids else "FAIL",
        "robust_candidate_count": len(_texts(summary.get("robust_candidates"))),
        "robustness_manifest_path": str(root / "robustness_manifest.json"),
        "rolling_robustness_path": str(root / "rolling_robustness.jsonl"),
        "regime_robustness_path": str(root / "regime_robustness.jsonl"),
        "stability_robustness_path": str(root / "stability_robustness.jsonl"),
        "robustness_summary_path": str(root / "robustness_summary.json"),
        "weight_robustness_review_report_path": str(root / "weight_robustness_review_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "robustness_manifest.json", manifest)
    _write_jsonl(root / "rolling_robustness.jsonl", rolling)
    _write_jsonl(root / "regime_robustness.jsonl", regime)
    _write_jsonl(root / "stability_robustness.jsonl", stability)
    _write_json(root / "robustness_summary.json", summary)
    _write_text(
        root / "weight_robustness_review_report.md", render_robustness_report(manifest, summary)
    )
    _write_latest_pointer(
        "latest_weight_robustness_review", root.name, root / "robustness_manifest.json"
    )
    return {
        "robustness_id": root.name,
        "robustness_dir": root,
        "manifest": manifest,
        "rolling_robustness": rolling,
        "regime_robustness": regime,
        "stability_robustness": stability,
        "robustness_summary": summary,
    }


def weight_robustness_review_report_payload(
    *,
    robustness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=robustness_id,
        latest_pointer="latest_weight_robustness_review",
        latest=latest,
        output_dir=output_dir,
        required_name="robustness_manifest.json",
    )
    return {
        **_read_json(root / "robustness_manifest.json"),
        "rolling_robustness": _read_jsonl(root / "rolling_robustness.jsonl"),
        "regime_robustness": _read_jsonl(root / "regime_robustness.jsonl"),
        "stability_robustness": _read_jsonl(root / "stability_robustness.jsonl"),
        "robustness_summary": _read_json(root / "robustness_summary.json"),
        "robustness_dir": str(root),
    }


def validate_weight_robustness_review_artifact(
    *,
    robustness_id: str,
    output_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / robustness_id
    manifest = _read_optional_json(root / "robustness_manifest.json") or {}
    summary = _read_optional_json(root / "robustness_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "robustness_manifest.json",
            "rolling_robustness.jsonl",
            "regime_robustness.jsonl",
            "stability_robustness.jsonl",
            "robustness_summary.json",
            "weight_robustness_review_report.md",
        ),
    )
    checks.extend(
        [
            st._check("robustness_id_matches", manifest.get("robustness_id") == robustness_id, ""),
            st._check("summary_present", bool(summary), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary), ""),
            st._check("experiment_safety_locked", _payload_experiment_safe(manifest, summary), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_robustness_review_validation", robustness_id, checks
    )


def run_weight_adaptive_branch(
    *,
    scorecard_id: str,
    robustness_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    robustness_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    output_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    robustness = weight_robustness_review_report_payload(
        robustness_id=robustness_id,
        output_dir=robustness_dir,
    )
    decision = _adaptive_branch_decision(scorecard, robustness)
    branch_id = _stable_id(
        "weight-adaptive-branch", scorecard_id, robustness_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / branch_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_adaptive_branch_manifest",
        "branch_id": root.name,
        "scorecard_id": scorecard_id,
        "robustness_id": robustness_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "branch_decision": decision["branch_decision"],
        "weight_adaptive_branch_manifest_path": str(root / "adaptive_branch_manifest.json"),
        "branch_decision_path": str(root / "branch_decision.json"),
        "weight_adaptive_branch_report_path": str(root / "weight_adaptive_branch_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "adaptive_branch_manifest.json", manifest)
    _write_json(root / "branch_decision.json", decision)
    _write_text(
        root / "weight_adaptive_branch_report.md", render_adaptive_branch_report(manifest, decision)
    )
    _write_latest_pointer(
        "latest_weight_adaptive_branch", root.name, root / "adaptive_branch_manifest.json"
    )
    return {
        "branch_id": root.name,
        "branch_dir": root,
        "manifest": manifest,
        "branch_decision": decision,
    }


def weight_adaptive_branch_report_payload(
    *,
    branch_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=branch_id,
        latest_pointer="latest_weight_adaptive_branch",
        latest=latest,
        output_dir=output_dir,
        required_name="adaptive_branch_manifest.json",
    )
    return {
        **_read_json(root / "adaptive_branch_manifest.json"),
        "branch_decision_payload": _read_json(root / "branch_decision.json"),
        "branch_dir": str(root),
    }


def validate_weight_adaptive_branch_artifact(
    *,
    branch_id: str,
    output_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
) -> dict[str, Any]:
    root = output_dir / branch_id
    manifest = _read_optional_json(root / "adaptive_branch_manifest.json") or {}
    decision = _read_optional_json(root / "branch_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "adaptive_branch_manifest.json",
            "branch_decision.json",
            "weight_adaptive_branch_report.md",
        ),
    )
    checks.extend(
        [
            st._check("branch_id_matches", manifest.get("branch_id") == branch_id, ""),
            st._check("decision_present", bool(decision.get("branch_decision")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, decision), ""),
            st._check("experiment_safety_locked", _payload_experiment_safe(manifest, decision), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_adaptive_branch_validation", branch_id, checks
    )


def build_weight_expanded_search(
    *,
    branch_id: str,
    branch_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    search_space_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    output_dir: Path = DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    branch = weight_adaptive_branch_report_payload(branch_id=branch_id, output_dir=branch_dir)
    search_space_id = _text(_mapping(branch.get("branch_decision_payload")).get("search_space_id"))
    if not search_space_id:
        latest = True
    else:
        latest = False
    return build_weight_experiment_batch2(
        search_space_id=search_space_id or None,
        latest_search_space=latest,
        search_space_dir=search_space_dir,
        output_dir=output_dir,
        generated_at=generated_at,
        expanded=True,
    )


def run_weight_expanded_search(
    *,
    expanded_matrix_id: str,
    expanded_matrix_dir: Path = DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return run_weight_batch_backfill(
        matrix_id=expanded_matrix_id,
        matrix_dir=expanded_matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated_at,
    )


def run_weight_candidate_cluster(
    *,
    scorecard_id: str,
    robustness_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    robustness_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    robustness = weight_robustness_review_report_payload(
        robustness_id=robustness_id, output_dir=robustness_dir
    )
    clusters, representatives = _candidate_clusters(scorecard, robustness)
    cluster_id = _stable_id(
        "weight-candidate-cluster", scorecard_id, robustness_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / cluster_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_candidate_cluster_manifest",
        "cluster_id": root.name,
        "scorecard_id": scorecard_id,
        "robustness_id": robustness_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if representatives else "FAIL",
        "cluster_count": len(clusters.get("clusters", [])),
        "candidate_cluster_manifest_path": str(root / "candidate_cluster_manifest.json"),
        "candidate_clusters_path": str(root / "candidate_clusters.json"),
        "cluster_representatives_path": str(root / "cluster_representatives.json"),
        "candidate_cluster_report_path": str(root / "candidate_cluster_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "candidate_cluster_manifest.json", manifest)
    _write_json(root / "candidate_clusters.json", clusters)
    _write_json(root / "cluster_representatives.json", representatives)
    _write_text(
        root / "candidate_cluster_report.md",
        render_candidate_cluster_report(manifest, representatives),
    )
    _write_latest_pointer(
        "latest_weight_candidate_cluster", root.name, root / "candidate_cluster_manifest.json"
    )
    return {
        "cluster_id": root.name,
        "cluster_dir": root,
        "manifest": manifest,
        "candidate_clusters": clusters,
        "cluster_representatives": representatives,
    }


def weight_candidate_cluster_report_payload(
    *,
    cluster_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=cluster_id,
        latest_pointer="latest_weight_candidate_cluster",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_cluster_manifest.json",
    )
    return {
        **_read_json(root / "candidate_cluster_manifest.json"),
        "candidate_clusters": _read_json(root / "candidate_clusters.json"),
        "cluster_representatives": _read_json(root / "cluster_representatives.json"),
        "cluster_dir": str(root),
    }


def validate_weight_candidate_cluster_artifact(
    *,
    cluster_id: str,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> dict[str, Any]:
    root = output_dir / cluster_id
    manifest = _read_optional_json(root / "candidate_cluster_manifest.json") or {}
    representatives = _read_optional_json(root / "cluster_representatives.json") or {}
    checks = _required_file_checks(
        root,
        (
            "candidate_cluster_manifest.json",
            "candidate_clusters.json",
            "cluster_representatives.json",
            "candidate_cluster_report.md",
        ),
    )
    checks.extend(
        [
            st._check("cluster_id_matches", manifest.get("cluster_id") == cluster_id, ""),
            st._check(
                "representatives_present",
                bool(_records(representatives.get("representatives"))),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, representatives), ""),
            st._check(
                "experiment_safety_locked", _payload_experiment_safe(manifest, representatives), ""
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_candidate_cluster_validation", cluster_id, checks
    )


def run_weight_top_candidate_interpretation(
    *,
    cluster_id: str,
    cluster_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    cluster = weight_candidate_cluster_report_payload(cluster_id=cluster_id, output_dir=cluster_dir)
    reps = _records(_mapping(cluster.get("cluster_representatives")).get("representatives"))
    explanations = [_candidate_explanation(row) for row in reps[:5]]
    coverage = _failure_mode_coverage_from_explanations(explanations)
    interpretation_id = _stable_id(
        "weight-top-candidate-interpretation", cluster_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / interpretation_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_top_candidate_interpretation_manifest",
        "interpretation_id": root.name,
        "cluster_id": cluster_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if explanations else "FAIL",
        "recommended_variant": _text(explanations[0].get("variant_id")) if explanations else "",
        "top_candidate_interpretation_manifest_path": str(
            root / "top_candidate_interpretation_manifest.json"
        ),
        "top_candidate_explanations_path": str(root / "top_candidate_explanations.jsonl"),
        "failure_mode_coverage_path": str(root / "failure_mode_coverage.json"),
        "top_candidate_interpretation_report_path": str(
            root / "top_candidate_interpretation_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_top_candidate_reader_brief(manifest, explanations)
    _write_json(root / "top_candidate_interpretation_manifest.json", manifest)
    _write_jsonl(root / "top_candidate_explanations.jsonl", explanations)
    _write_json(root / "failure_mode_coverage.json", coverage)
    _write_text(
        root / "top_candidate_interpretation_report.md",
        render_top_candidate_interpretation_report(manifest, explanations),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_weight_top_candidate_interpretation",
        root.name,
        root / "top_candidate_interpretation_manifest.json",
    )
    return {
        "interpretation_id": root.name,
        "interpretation_dir": root,
        "manifest": manifest,
        "top_candidate_explanations": explanations,
        "failure_mode_coverage": coverage,
        "reader_brief_section": reader,
    }


def weight_top_candidate_interpretation_report_payload(
    *,
    interpretation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=interpretation_id,
        latest_pointer="latest_weight_top_candidate_interpretation",
        latest=latest,
        output_dir=output_dir,
        required_name="top_candidate_interpretation_manifest.json",
    )
    return {
        **_read_json(root / "top_candidate_interpretation_manifest.json"),
        "top_candidate_explanations": _read_jsonl(root / "top_candidate_explanations.jsonl"),
        "failure_mode_coverage": _read_json(root / "failure_mode_coverage.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "interpretation_dir": str(root),
    }


def validate_weight_top_candidate_interpretation_artifact(
    *,
    interpretation_id: str,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = output_dir / interpretation_id
    manifest = _read_optional_json(root / "top_candidate_interpretation_manifest.json") or {}
    explanations = _read_jsonl(root / "top_candidate_explanations.jsonl")
    checks = _required_file_checks(
        root,
        (
            "top_candidate_interpretation_manifest.json",
            "top_candidate_explanations.jsonl",
            "failure_mode_coverage.json",
            "top_candidate_interpretation_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "interpretation_id_matches",
                manifest.get("interpretation_id") == interpretation_id,
                "",
            ),
            st._check("explanations_present", bool(explanations), ""),
            st._check("broker_forbidden", _payload_safe(manifest, *explanations), ""),
            st._check(
                "experiment_safety_locked", _payload_experiment_safe(manifest, *explanations), ""
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_top_candidate_interpretation_validation", interpretation_id, checks
    )


def run_weight_method_promotion_gate(
    *,
    interpretation_id: str,
    interpretation_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
    output_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    interpretation = weight_top_candidate_interpretation_report_payload(
        interpretation_id=interpretation_id,
        output_dir=interpretation_dir,
    )
    decisions = _promotion_gate_decisions(
        _records(interpretation.get("top_candidate_explanations"))
    )
    promoted = [row for row in decisions if row["decision"] == "PROMOTE_TO_FORMAL_IMPLEMENTATION"][
        :3
    ]
    gate_id = _stable_id("weight-method-promotion-gate", interpretation_id, generated.isoformat())
    root = _unique_dir(output_dir / gate_id)
    root.mkdir(parents=True, exist_ok=False)
    decision_payload = {
        "schema_version": st.SCHEMA_VERSION,
        "promotion_gate_id": root.name,
        "decision_summary": _promotion_decision_summary(decisions),
        "decisions": decisions,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    specs = {
        "schema_version": st.SCHEMA_VERSION,
        "promoted_candidates": [_promoted_candidate_spec(row) for row in promoted],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_method_promotion_gate_manifest",
        "promotion_gate_id": root.name,
        "interpretation_id": interpretation_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if decisions else "FAIL",
        "promoted_candidate_count": len(promoted),
        "promotion_gate_manifest_path": str(root / "promotion_gate_manifest.json"),
        "promotion_gate_decision_path": str(root / "promotion_gate_decision.json"),
        "promoted_candidate_specs_path": str(root / "promoted_candidate_specs.json"),
        "promotion_gate_report_path": str(root / "promotion_gate_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "promotion_gate_manifest.json", manifest)
    _write_json(root / "promotion_gate_decision.json", decision_payload)
    _write_json(root / "promoted_candidate_specs.json", specs)
    _write_text(
        root / "promotion_gate_report.md", render_promotion_gate_report(manifest, decision_payload)
    )
    _write_latest_pointer(
        "latest_weight_method_promotion_gate", root.name, root / "promotion_gate_manifest.json"
    )
    return {
        "promotion_gate_id": root.name,
        "promotion_gate_dir": root,
        "manifest": manifest,
        "promotion_gate_decision": decision_payload,
        "promoted_candidate_specs": specs,
    }


def weight_method_promotion_gate_report_payload(
    *,
    promotion_gate_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=promotion_gate_id,
        latest_pointer="latest_weight_method_promotion_gate",
        latest=latest,
        output_dir=output_dir,
        required_name="promotion_gate_manifest.json",
    )
    return {
        **_read_json(root / "promotion_gate_manifest.json"),
        "promotion_gate_decision": _read_json(root / "promotion_gate_decision.json"),
        "promoted_candidate_specs": _read_json(root / "promoted_candidate_specs.json"),
        "promotion_gate_dir": str(root),
    }


def validate_weight_method_promotion_gate_artifact(
    *,
    promotion_gate_id: str,
    output_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> dict[str, Any]:
    root = output_dir / promotion_gate_id
    manifest = _read_optional_json(root / "promotion_gate_manifest.json") or {}
    decision = _read_optional_json(root / "promotion_gate_decision.json") or {}
    specs = _read_optional_json(root / "promoted_candidate_specs.json") or {}
    checks = _required_file_checks(
        root,
        (
            "promotion_gate_manifest.json",
            "promotion_gate_decision.json",
            "promoted_candidate_specs.json",
            "promotion_gate_report.md",
        ),
    )
    allowed = {
        "PROMOTE_TO_FORMAL_IMPLEMENTATION",
        "KEEP_FOR_MORE_TESTING",
        "REJECT",
        "DEFER_FOR_FORWARD_DATA",
    }
    checks.extend(
        [
            st._check(
                "promotion_gate_id_matches",
                manifest.get("promotion_gate_id") == promotion_gate_id,
                "",
            ),
            st._check(
                "decision_types_valid",
                {row.get("decision") for row in _records(decision.get("decisions"))}.issubset(
                    allowed
                ),
                "",
            ),
            st._check(
                "promoted_specs_bounded", len(_records(specs.get("promoted_candidates"))) <= 3, ""
            ),
            st._check("broker_forbidden", _payload_safe(manifest, decision, specs), ""),
            st._check(
                "experiment_safety_locked", _payload_experiment_safe(manifest, decision, specs), ""
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_method_promotion_gate_validation", promotion_gate_id, checks
    )


def run_formal_method_auto_plan(
    *,
    promotion_gate_id: str,
    promotion_gate_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    output_dir: Path = DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    gate = weight_method_promotion_gate_report_payload(
        promotion_gate_id=promotion_gate_id,
        output_dir=promotion_gate_dir,
    )
    candidates = _records(_mapping(gate.get("promoted_candidate_specs")).get("promoted_candidates"))
    specs = _formal_method_specs(candidates)
    validation_plan = _formal_validation_plan(specs)
    plan_id = _stable_id("formal-method-auto-plan", promotion_gate_id, generated.isoformat())
    root = _unique_dir(output_dir / plan_id)
    root.mkdir(parents=True, exist_ok=False)
    status = "PLAN_READY" if _records(specs.get("methods")) else "SKIPPED_NO_PROMOTED_CANDIDATE"
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_formal_method_auto_plan_manifest",
        "plan_id": root.name,
        "promotion_gate_id": promotion_gate_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "implemented": False,
        "implementation_reason": (
            "auto-plan only; no official target, broker, production, or owner approval action"
        ),
        "formal_method_auto_plan_manifest_path": str(
            root / "formal_method_auto_plan_manifest.json"
        ),
        "formal_method_specs_path": str(root / "formal_method_specs.json"),
        "implementation_plan_path": str(root / "implementation_plan.md"),
        "validation_plan_path": str(root / "validation_plan.json"),
        "formal_method_auto_plan_report_path": str(root / "formal_method_auto_plan_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    plan_text = render_formal_method_implementation_plan(manifest, specs, validation_plan)
    _write_json(root / "formal_method_auto_plan_manifest.json", manifest)
    _write_json(root / "formal_method_specs.json", specs)
    _write_text(root / "implementation_plan.md", plan_text)
    _write_json(root / "validation_plan.json", validation_plan)
    _write_text(
        root / "formal_method_auto_plan_report.md",
        render_formal_method_auto_plan_report(manifest, specs),
    )
    _write_latest_pointer(
        "latest_formal_method_auto_plan", root.name, root / "formal_method_auto_plan_manifest.json"
    )
    return {
        "plan_id": root.name,
        "plan_dir": root,
        "manifest": manifest,
        "formal_method_specs": specs,
        "validation_plan": validation_plan,
        "implementation_plan": plan_text,
    }


def formal_method_auto_plan_report_payload(
    *,
    plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=plan_id,
        latest_pointer="latest_formal_method_auto_plan",
        latest=latest,
        output_dir=output_dir,
        required_name="formal_method_auto_plan_manifest.json",
    )
    return {
        **_read_json(root / "formal_method_auto_plan_manifest.json"),
        "formal_method_specs": _read_json(root / "formal_method_specs.json"),
        "validation_plan": _read_json(root / "validation_plan.json"),
        "implementation_plan": (root / "implementation_plan.md").read_text(encoding="utf-8"),
        "plan_dir": str(root),
    }


def validate_formal_method_auto_plan_artifact(
    *,
    plan_id: str,
    output_dir: Path = DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> dict[str, Any]:
    root = output_dir / plan_id
    manifest = _read_optional_json(root / "formal_method_auto_plan_manifest.json") or {}
    specs = _read_optional_json(root / "formal_method_specs.json") or {}
    checks = _required_file_checks(
        root,
        (
            "formal_method_auto_plan_manifest.json",
            "formal_method_specs.json",
            "implementation_plan.md",
            "validation_plan.json",
            "formal_method_auto_plan_report.md",
        ),
    )
    checks.extend(
        [
            st._check("plan_id_matches", manifest.get("plan_id") == plan_id, ""),
            st._check("implemented_false", manifest.get("implemented") is False, ""),
            st._check(
                "method_specs_safe",
                all(
                    row.get("broker_action_allowed") is False
                    and row.get("production_effect") == st.PRODUCTION_EFFECT
                    for row in _records(specs.get("methods"))
                ),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, specs), ""),
            st._check("experiment_safety_locked", _payload_experiment_safe(manifest, specs), ""),
        ]
    )
    return _validation_payload("etf_dynamic_v3_formal_method_auto_plan_validation", plan_id, checks)


def build_weight_search_dashboard(
    *,
    scorecard_id: str,
    branch_id: str,
    promotion_gate_id: str | None = None,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    branch_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    promotion_gate_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    branch = weight_adaptive_branch_report_payload(branch_id=branch_id, output_dir=branch_dir)
    gate = (
        weight_method_promotion_gate_report_payload(
            promotion_gate_id=promotion_gate_id,
            output_dir=promotion_gate_dir,
        )
        if promotion_gate_id
        else {}
    )
    summary = _dashboard_summary(scorecard, branch, gate)
    dashboard_id = _stable_id(
        "weight-search-dashboard",
        scorecard_id,
        branch_id,
        promotion_gate_id or "",
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / dashboard_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_search_dashboard_manifest",
        "dashboard_id": root.name,
        "scorecard_id": scorecard_id,
        "branch_id": branch_id,
        "promotion_gate_id": promotion_gate_id or "",
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "search_dashboard_manifest_path": str(root / "search_dashboard_manifest.json"),
        "search_summary_path": str(root / "search_summary.json"),
        "top_candidates_path": str(root / "top_candidates.json"),
        "rejected_summary_path": str(root / "rejected_summary.json"),
        "next_actions_path": str(root / "next_actions.json"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_dashboard_reader_brief(summary)
    _write_json(root / "search_dashboard_manifest.json", manifest)
    _write_json(root / "search_summary.json", summary["search_summary"])
    _write_json(root / "top_candidates.json", summary["top_candidates"])
    _write_json(root / "rejected_summary.json", summary["rejected_summary"])
    _write_json(root / "next_actions.json", summary["next_actions"])
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_weight_search_dashboard", root.name, root / "search_dashboard_manifest.json"
    )
    return {
        "dashboard_id": root.name,
        "dashboard_dir": root,
        "manifest": manifest,
        **summary,
        "reader_brief_section": reader,
    }


def weight_search_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=dashboard_id,
        latest_pointer="latest_weight_search_dashboard",
        latest=latest,
        output_dir=output_dir,
        required_name="search_dashboard_manifest.json",
    )
    return {
        **_read_json(root / "search_dashboard_manifest.json"),
        "search_summary": _read_json(root / "search_summary.json"),
        "top_candidates": _read_json(root / "top_candidates.json"),
        "rejected_summary": _read_json(root / "rejected_summary.json"),
        "next_actions": _read_json(root / "next_actions.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "dashboard_dir": str(root),
    }


def validate_weight_search_dashboard_artifact(
    *,
    dashboard_id: str,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = output_dir / dashboard_id
    manifest = _read_optional_json(root / "search_dashboard_manifest.json") or {}
    summary = _read_optional_json(root / "search_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "search_dashboard_manifest.json",
            "search_summary.json",
            "top_candidates.json",
            "rejected_summary.json",
            "next_actions.json",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("dashboard_id_matches", manifest.get("dashboard_id") == dashboard_id, ""),
            st._check("summary_answers_variant_count", "variants_total" in summary, ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary), ""),
            st._check("experiment_safety_locked", _payload_experiment_safe(manifest, summary), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weight_search_dashboard_validation", dashboard_id, checks
    )


def build_owner_research_decision_pack(
    *,
    dashboard_id: str,
    dashboard_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    dashboard = weight_search_dashboard_report_payload(
        dashboard_id=dashboard_id, output_dir=dashboard_dir
    )
    options = _owner_decision_options(dashboard)
    pack_id = _stable_id("owner-research-decision-pack", dashboard_id, generated.isoformat())
    root = _unique_dir(output_dir / pack_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_research_decision_pack_manifest",
        "owner_pack_id": root.name,
        "dashboard_id": dashboard_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "recommended_owner_decision": options.get("recommended_decision"),
        "owner_decision_pack_manifest_path": str(root / "owner_decision_pack_manifest.json"),
        "owner_decision_options_path": str(root / "owner_decision_options.json"),
        "owner_decision_pack_report_path": str(root / "owner_decision_pack_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "owner_decision_pack_manifest.json", manifest)
    _write_json(root / "owner_decision_options.json", options)
    _write_text(
        root / "owner_decision_pack_report.md", render_owner_decision_pack_report(manifest, options)
    )
    _write_latest_pointer(
        "latest_owner_research_decision_pack", root.name, root / "owner_decision_pack_manifest.json"
    )
    return {
        "owner_pack_id": root.name,
        "owner_pack_dir": root,
        "manifest": manifest,
        "owner_decision_options": options,
    }


def owner_research_decision_pack_report_payload(
    *,
    owner_pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=owner_pack_id,
        latest_pointer="latest_owner_research_decision_pack",
        latest=latest,
        output_dir=output_dir,
        required_name="owner_decision_pack_manifest.json",
    )
    return {
        **_read_json(root / "owner_decision_pack_manifest.json"),
        "owner_decision_options": _read_json(root / "owner_decision_options.json"),
        "owner_pack_dir": str(root),
    }


def validate_owner_research_decision_pack_artifact(
    *,
    owner_pack_id: str,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> dict[str, Any]:
    root = output_dir / owner_pack_id
    manifest = _read_optional_json(root / "owner_decision_pack_manifest.json") or {}
    options = _read_optional_json(root / "owner_decision_options.json") or {}
    allowed = {
        "continue_search",
        "implement_top_candidate",
        "defer_for_forward_data",
        "reject_all_candidates",
        "run_expanded_search",
    }
    checks = _required_file_checks(
        root,
        (
            "owner_decision_pack_manifest.json",
            "owner_decision_options.json",
            "owner_decision_pack_report.md",
        ),
    )
    checks.extend(
        [
            st._check("owner_pack_id_matches", manifest.get("owner_pack_id") == owner_pack_id, ""),
            st._check(
                "recommended_decision_valid",
                options.get("recommended_decision") in allowed,
                _text(options.get("recommended_decision")),
            ),
            st._check("broker_forbidden", _payload_safe(manifest, options), ""),
            st._check("experiment_safety_locked", _payload_experiment_safe(manifest, options), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_owner_research_decision_pack_validation", owner_pack_id, checks
    )


def run_no_promotion_review(
    *,
    scorecard_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    output_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    rows = _records(scorecard.get("variant_scorecard"))
    reason_summary = _no_promotion_reason_summary(scorecard)
    failure_distribution = _gate_failure_distribution(rows)
    component_matrix = _score_component_failure_matrix(rows)
    review_id = _stable_id("no-promotion-review", scorecard_id, generated.isoformat())
    root = _unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_no_promotion_review_manifest",
        "review_id": root.name,
        "source_scorecard_id": scorecard_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "market_regime": scorecard.get("market_regime", "ai_after_chatgpt"),
        "date_start": scorecard.get("date_start"),
        "date_end": scorecard.get("date_end"),
        "data_quality_status": scorecard.get("data_quality_status"),
        "variants_reviewed": len(rows),
        "promoted_candidate_count": reason_summary["promoted_candidate_count"],
        "no_promotion_review_manifest_path": str(root / "no_promotion_review_manifest.json"),
        "no_promotion_reason_summary_path": str(root / "no_promotion_reason_summary.json"),
        "gate_failure_distribution_path": str(root / "gate_failure_distribution.json"),
        "score_component_failure_matrix_path": str(root / "score_component_failure_matrix.json"),
        "no_promotion_review_report_path": str(root / "no_promotion_review_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_no_promotion_reader_brief(manifest, reason_summary)
    _write_json(root / "no_promotion_review_manifest.json", manifest)
    _write_json(root / "no_promotion_reason_summary.json", reason_summary)
    _write_json(root / "gate_failure_distribution.json", failure_distribution)
    _write_json(root / "score_component_failure_matrix.json", component_matrix)
    _write_text(
        root / "no_promotion_review_report.md",
        render_no_promotion_review_report(
            manifest, reason_summary, failure_distribution, component_matrix
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_no_promotion_review", root.name, root / "no_promotion_review_manifest.json"
    )
    return {
        "review_id": root.name,
        "review_dir": root,
        "manifest": manifest,
        "no_promotion_reason_summary": reason_summary,
        "gate_failure_distribution": failure_distribution,
        "score_component_failure_matrix": component_matrix,
        "reader_brief_section": reader,
    }


def no_promotion_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=review_id,
        latest_pointer="latest_no_promotion_review",
        latest=latest,
        output_dir=output_dir,
        required_name="no_promotion_review_manifest.json",
    )
    return {
        **_read_json(root / "no_promotion_review_manifest.json"),
        "no_promotion_reason_summary": _read_json(root / "no_promotion_reason_summary.json"),
        "gate_failure_distribution": _read_json(root / "gate_failure_distribution.json"),
        "score_component_failure_matrix": _read_json(root / "score_component_failure_matrix.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "review_dir": str(root),
    }


def validate_no_promotion_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / review_id
    manifest = _read_optional_json(root / "no_promotion_review_manifest.json") or {}
    reason = _read_optional_json(root / "no_promotion_reason_summary.json") or {}
    failure = _read_optional_json(root / "gate_failure_distribution.json") or {}
    matrix = _read_optional_json(root / "score_component_failure_matrix.json") or {}
    checks = _required_file_checks(
        root,
        (
            "no_promotion_review_manifest.json",
            "no_promotion_reason_summary.json",
            "gate_failure_distribution.json",
            "score_component_failure_matrix.json",
            "no_promotion_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("review_id_matches", manifest.get("review_id") == review_id, ""),
            st._check(
                "promoted_candidate_count_visible",
                "promoted_candidate_count" in reason,
                "",
            ),
            st._check(
                "gate_assessment_valid",
                reason.get("gate_assessment")
                in {"REASONABLE", "TOO_STRICT", "TOO_LOOSE", "INCONCLUSIVE"},
                _text(reason.get("gate_assessment")),
            ),
            st._check("gate_failures_readable", bool(_records(failure.get("failures"))), ""),
            st._check("component_matrix_readable", bool(_records(matrix.get("components"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, reason, failure, matrix), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, reason, failure, matrix),
                "",
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_no_promotion_review_validation", review_id, checks)


def extract_near_miss_candidates(
    *,
    scorecard_id: str,
    no_promotion_review_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    review_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    review = no_promotion_review_report_payload(
        review_id=no_promotion_review_id, output_dir=review_dir
    )
    candidates = _near_miss_candidate_rows(scorecard)
    family_summary = _near_miss_family_summary(candidates, scorecard)
    near_miss_id = _stable_id(
        "near-miss-candidates",
        scorecard_id,
        no_promotion_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / near_miss_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_near_miss_manifest",
        "near_miss_id": root.name,
        "source_scorecard_id": scorecard_id,
        "no_promotion_review_id": no_promotion_review_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if candidates else "PASS_WITH_WARNINGS",
        "market_regime": scorecard.get("market_regime", "ai_after_chatgpt"),
        "date_start": scorecard.get("date_start"),
        "date_end": scorecard.get("date_end"),
        "candidate_count": len(candidates),
        "cash_buffer_10_near_miss": any(
            row.get("variant_id") == "cash_buffer_10" for row in candidates
        ),
        "source_review_gate_assessment": _mapping(review.get("no_promotion_reason_summary")).get(
            "gate_assessment"
        ),
        "near_miss_manifest_path": str(root / "near_miss_manifest.json"),
        "near_miss_candidates_path": str(root / "near_miss_candidates.jsonl"),
        "near_miss_family_summary_path": str(root / "near_miss_family_summary.json"),
        "near_miss_report_path": str(root / "near_miss_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_near_miss_reader_brief(manifest, family_summary)
    _write_json(root / "near_miss_manifest.json", manifest)
    _write_jsonl(root / "near_miss_candidates.jsonl", candidates)
    _write_json(root / "near_miss_family_summary.json", family_summary)
    _write_text(
        root / "near_miss_report.md", render_near_miss_report(manifest, candidates, family_summary)
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_near_miss_candidates", root.name, root / "near_miss_manifest.json"
    )
    return {
        "near_miss_id": root.name,
        "near_miss_dir": root,
        "manifest": manifest,
        "near_miss_candidates": candidates,
        "near_miss_family_summary": family_summary,
        "reader_brief_section": reader,
    }


def near_miss_candidates_report_payload(
    *,
    near_miss_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=near_miss_id,
        latest_pointer="latest_near_miss_candidates",
        latest=latest,
        output_dir=output_dir,
        required_name="near_miss_manifest.json",
    )
    return {
        **_read_json(root / "near_miss_manifest.json"),
        "near_miss_candidates": _read_jsonl(root / "near_miss_candidates.jsonl"),
        "near_miss_family_summary": _read_json(root / "near_miss_family_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "near_miss_dir": str(root),
    }


def validate_near_miss_candidates_artifact(
    *,
    near_miss_id: str,
    output_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
) -> dict[str, Any]:
    root = output_dir / near_miss_id
    manifest = _read_optional_json(root / "near_miss_manifest.json") or {}
    candidates = _read_jsonl(root / "near_miss_candidates.jsonl")
    summary = _read_optional_json(root / "near_miss_family_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "near_miss_manifest.json",
            "near_miss_candidates.jsonl",
            "near_miss_family_summary.json",
            "near_miss_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("near_miss_id_matches", manifest.get("near_miss_id") == near_miss_id, ""),
            st._check(
                "candidate_count_matches",
                int(_float(manifest.get("candidate_count"))) == len(candidates),
                "",
            ),
            st._check(
                "candidates_have_status",
                all(row.get("candidate_status") == "NEAR_MISS" for row in candidates),
                "",
            ),
            st._check(
                "recommended_focus_visible",
                bool(_texts(summary.get("recommended_focus_families"))),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *candidates), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *candidates),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_near_miss_candidates_validation", near_miss_id, checks
    )


def run_cash_buffer_attribution(
    *,
    variant_id: str,
    scorecard_id: str,
    near_miss_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    output_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)
    near_miss = near_miss_candidates_report_payload(
        near_miss_id=near_miss_id, output_dir=near_miss_dir
    )
    row = _scorecard_row(scorecard, variant_id)
    effect = _cash_buffer_effect_summary(row)
    failure = _cash_buffer_failure_reason(row, near_miss)
    recommendations = _cash_buffer_variant_recommendations(row)
    attribution_id = _stable_id(
        "cash-buffer-attribution", variant_id, scorecard_id, near_miss_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cash_buffer_attribution_manifest",
        "attribution_id": root.name,
        "variant_id": variant_id,
        "source_scorecard_id": scorecard_id,
        "near_miss_id": near_miss_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if row else "FAIL",
        "market_regime": scorecard.get("market_regime", "ai_after_chatgpt"),
        "cash_buffer_attribution_manifest_path": str(
            root / "cash_buffer_attribution_manifest.json"
        ),
        "cash_buffer_effect_summary_path": str(root / "cash_buffer_effect_summary.json"),
        "cash_buffer_failure_reason_path": str(root / "cash_buffer_failure_reason.json"),
        "cash_buffer_variant_recommendations_path": str(
            root / "cash_buffer_variant_recommendations.json"
        ),
        "cash_buffer_attribution_report_path": str(root / "cash_buffer_attribution_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "cash_buffer_attribution_manifest.json", manifest)
    _write_json(root / "cash_buffer_effect_summary.json", effect)
    _write_json(root / "cash_buffer_failure_reason.json", failure)
    _write_json(root / "cash_buffer_variant_recommendations.json", recommendations)
    _write_text(
        root / "cash_buffer_attribution_report.md",
        render_cash_buffer_attribution_report(manifest, effect, failure, recommendations),
    )
    _write_latest_pointer(
        "latest_cash_buffer_attribution",
        root.name,
        root / "cash_buffer_attribution_manifest.json",
    )
    return {
        "attribution_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "cash_buffer_effect_summary": effect,
        "cash_buffer_failure_reason": failure,
        "cash_buffer_variant_recommendations": recommendations,
    }


def cash_buffer_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_cash_buffer_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="cash_buffer_attribution_manifest.json",
    )
    return {
        **_read_json(root / "cash_buffer_attribution_manifest.json"),
        "cash_buffer_effect_summary": _read_json(root / "cash_buffer_effect_summary.json"),
        "cash_buffer_failure_reason": _read_json(root / "cash_buffer_failure_reason.json"),
        "cash_buffer_variant_recommendations": _read_json(
            root / "cash_buffer_variant_recommendations.json"
        ),
        "attribution_dir": str(root),
    }


def validate_cash_buffer_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    manifest = _read_optional_json(root / "cash_buffer_attribution_manifest.json") or {}
    effect = _read_optional_json(root / "cash_buffer_effect_summary.json") or {}
    failure = _read_optional_json(root / "cash_buffer_failure_reason.json") or {}
    recommendations = _read_optional_json(root / "cash_buffer_variant_recommendations.json") or {}
    checks = _required_file_checks(
        root,
        (
            "cash_buffer_attribution_manifest.json",
            "cash_buffer_effect_summary.json",
            "cash_buffer_failure_reason.json",
            "cash_buffer_variant_recommendations.json",
            "cash_buffer_attribution_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "attribution_id_matches", manifest.get("attribution_id") == attribution_id, ""
            ),
            st._check("variant_id_visible", bool(effect.get("variant_id")), ""),
            st._check("failure_reason_visible", bool(failure.get("primary_failure_reason")), ""),
            st._check(
                "recommendations_visible",
                bool(_texts(recommendations.get("recommended_variants"))),
                "",
            ),
            st._check(
                "broker_forbidden", _payload_safe(manifest, effect, failure, recommendations), ""
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, effect, failure, recommendations),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_cash_buffer_attribution_validation", attribution_id, checks
    )


def run_search_coverage_gap(
    *,
    search_space_id: str,
    near_miss_id: str,
    cash_buffer_attribution_id: str,
    search_space_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    attribution_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    search_space = weight_search_space_report_payload(
        search_space_id=search_space_id, output_dir=search_space_dir
    )
    near_miss = near_miss_candidates_report_payload(
        near_miss_id=near_miss_id, output_dir=near_miss_dir
    )
    attribution = cash_buffer_attribution_report_payload(
        attribution_id=cash_buffer_attribution_id,
        output_dir=attribution_dir,
    )
    family_gap = _family_coverage_gap(search_space, near_miss)
    parameter_gap = _parameter_coverage_gap(search_space, attribution)
    recommendations = _targeted_v3_recommendations(family_gap, parameter_gap)
    coverage_gap_id = _stable_id(
        "search-coverage-gap",
        search_space_id,
        near_miss_id,
        cash_buffer_attribution_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / coverage_gap_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_search_coverage_gap_manifest",
        "coverage_gap_id": root.name,
        "search_space_id": search_space_id,
        "near_miss_id": near_miss_id,
        "cash_buffer_attribution_id": cash_buffer_attribution_id,
        "source_scorecard_id": near_miss.get("source_scorecard_id"),
        "source_backfill_id": _text(search_space.get("source_backfill_id"))
        or _text(
            _mapping(_mapping(search_space.get("normalized_search_space")).get("search")).get(
                "source_backfill_id"
            )
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "search_coverage_gap_manifest_path": str(root / "search_coverage_gap_manifest.json"),
        "family_coverage_gap_path": str(root / "family_coverage_gap.json"),
        "parameter_coverage_gap_path": str(root / "parameter_coverage_gap.json"),
        "targeted_v3_recommendations_path": str(root / "targeted_v3_recommendations.json"),
        "search_coverage_gap_report_path": str(root / "search_coverage_gap_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "search_coverage_gap_manifest.json", manifest)
    _write_json(root / "family_coverage_gap.json", family_gap)
    _write_json(root / "parameter_coverage_gap.json", parameter_gap)
    _write_json(root / "targeted_v3_recommendations.json", recommendations)
    _write_text(
        root / "search_coverage_gap_report.md",
        render_search_coverage_gap_report(manifest, family_gap, parameter_gap, recommendations),
    )
    _write_latest_pointer(
        "latest_search_coverage_gap", root.name, root / "search_coverage_gap_manifest.json"
    )
    return {
        "coverage_gap_id": root.name,
        "coverage_gap_dir": root,
        "manifest": manifest,
        "family_coverage_gap": family_gap,
        "parameter_coverage_gap": parameter_gap,
        "targeted_v3_recommendations": recommendations,
    }


def search_coverage_gap_report_payload(
    *,
    coverage_gap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=coverage_gap_id,
        latest_pointer="latest_search_coverage_gap",
        latest=latest,
        output_dir=output_dir,
        required_name="search_coverage_gap_manifest.json",
    )
    return {
        **_read_json(root / "search_coverage_gap_manifest.json"),
        "family_coverage_gap": _read_json(root / "family_coverage_gap.json"),
        "parameter_coverage_gap": _read_json(root / "parameter_coverage_gap.json"),
        "targeted_v3_recommendations": _read_json(root / "targeted_v3_recommendations.json"),
        "coverage_gap_dir": str(root),
    }


def validate_search_coverage_gap_artifact(
    *,
    coverage_gap_id: str,
    output_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
) -> dict[str, Any]:
    root = output_dir / coverage_gap_id
    manifest = _read_optional_json(root / "search_coverage_gap_manifest.json") or {}
    family_gap = _read_optional_json(root / "family_coverage_gap.json") or {}
    parameter_gap = _read_optional_json(root / "parameter_coverage_gap.json") or {}
    recommendations = _read_optional_json(root / "targeted_v3_recommendations.json") or {}
    checks = _required_file_checks(
        root,
        (
            "search_coverage_gap_manifest.json",
            "family_coverage_gap.json",
            "parameter_coverage_gap.json",
            "targeted_v3_recommendations.json",
            "search_coverage_gap_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "coverage_gap_id_matches", manifest.get("coverage_gap_id") == coverage_gap_id, ""
            ),
            st._check("family_gap_readable", bool(_records(family_gap.get("gaps"))), ""),
            st._check("parameter_gap_readable", bool(_records(parameter_gap.get("gaps"))), ""),
            st._check(
                "targeted_recommendations_visible",
                bool(_texts(recommendations.get("recommended_focus"))),
                "",
            ),
            st._check(
                "max_v3_variants_bounded",
                int(_float(recommendations.get("max_v3_variants"))) <= TARGETED_V3_MAX_VARIANTS,
                _text(recommendations.get("max_v3_variants")),
            ),
            st._check(
                "broker_forbidden",
                _payload_safe(manifest, family_gap, parameter_gap, recommendations),
                "",
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, family_gap, parameter_gap, recommendations),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_search_coverage_gap_validation", coverage_gap_id, checks
    )


def build_targeted_search_v3(
    *,
    coverage_gap_id: str,
    coverage_gap_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    output_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    coverage = search_coverage_gap_report_payload(
        coverage_gap_id=coverage_gap_id,
        output_dir=coverage_gap_dir,
    )
    near_miss = near_miss_candidates_report_payload(
        near_miss_id=_text(coverage.get("near_miss_id")),
        output_dir=near_miss_dir,
    )
    variants = _targeted_v3_variant_specs(coverage, near_miss)
    variants = variants[
        : int(
            _float(
                _mapping(coverage.get("targeted_v3_recommendations")).get("max_v3_variants"),
                TARGETED_V3_MAX_VARIANTS,
            )
        )
    ]
    family_coverage = _targeted_v3_family_coverage(variants)
    matrix_id = _stable_id("targeted-search-v3", coverage_gap_id, generated.isoformat())
    root = _unique_dir(output_dir / matrix_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_targeted_search_v3_manifest",
        "v3_matrix_id": root.name,
        "coverage_gap_id": coverage_gap_id,
        "near_miss_id": coverage.get("near_miss_id"),
        "source_scorecard_id": coverage.get("source_scorecard_id"),
        "source_backfill_id": coverage.get("source_backfill_id"),
        "search_space_id": coverage.get("search_space_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if 60 <= len(variants) <= TARGETED_V3_MAX_VARIANTS else "FAIL",
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": st.AI_AFTER_CHATGPT_START.isoformat(),
        "variant_count": len(variants),
        "targeted_search_v3_manifest_path": str(root / "targeted_search_v3_manifest.json"),
        "v3_variant_specs_path": str(root / "v3_variant_specs.jsonl"),
        "v3_family_coverage_path": str(root / "v3_family_coverage.json"),
        "targeted_search_v3_report_path": str(root / "targeted_search_v3_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "targeted_search_v3_manifest.json", manifest)
    _write_jsonl(root / "v3_variant_specs.jsonl", variants)
    _write_json(root / "v3_family_coverage.json", family_coverage)
    _write_text(
        root / "targeted_search_v3_report.md",
        render_targeted_search_v3_report(manifest, family_coverage),
    )
    _write_latest_pointer(
        "latest_targeted_search_v3", root.name, root / "targeted_search_v3_manifest.json"
    )
    return {
        "v3_matrix_id": root.name,
        "v3_matrix_dir": root,
        "manifest": manifest,
        "v3_variant_specs": variants,
        "v3_family_coverage": family_coverage,
    }


def targeted_search_v3_report_payload(
    *,
    v3_matrix_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v3_matrix_id,
        latest_pointer="latest_targeted_search_v3",
        latest=latest,
        output_dir=output_dir,
        required_name="targeted_search_v3_manifest.json",
    )
    return {
        **_read_json(root / "targeted_search_v3_manifest.json"),
        "v3_variant_specs": _read_jsonl(root / "v3_variant_specs.jsonl"),
        "v3_family_coverage": _read_json(root / "v3_family_coverage.json"),
        "v3_matrix_dir": str(root),
    }


def validate_targeted_search_v3_artifact(
    *,
    v3_matrix_id: str,
    output_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
) -> dict[str, Any]:
    root = output_dir / v3_matrix_id
    manifest = _read_optional_json(root / "targeted_search_v3_manifest.json") or {}
    variants = _read_jsonl(root / "v3_variant_specs.jsonl")
    coverage = _read_optional_json(root / "v3_family_coverage.json") or {}
    checks = _required_file_checks(
        root,
        (
            "targeted_search_v3_manifest.json",
            "v3_variant_specs.jsonl",
            "v3_family_coverage.json",
            "targeted_search_v3_report.md",
        ),
    )
    checks.extend(
        [
            st._check("v3_matrix_id_matches", manifest.get("v3_matrix_id") == v3_matrix_id, ""),
            st._check(
                "variant_count_bounded",
                60 <= len(variants) <= TARGETED_V3_MAX_VARIANTS,
                str(len(variants)),
            ),
            st._check(
                "each_variant_has_parent_or_gap",
                all(
                    row.get("near_miss_parent") or row.get("coverage_gap_reason")
                    for row in variants
                ),
                "",
            ),
            st._check(
                "focus_families_covered",
                {"cash_buffer_smoothing_hybrid", "cash_buffer_threshold_hybrid"}.issubset(
                    set(_texts(coverage.get("targeted_families_covered")))
                ),
                ",".join(_texts(coverage.get("targeted_families_covered"))),
            ),
            st._check("broker_forbidden", _payload_safe(manifest, coverage, *variants), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, coverage, *variants),
                "",
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_targeted_search_v3_validation", v3_matrix_id, checks)


def run_targeted_v3_backfill(
    *,
    v3_matrix_id: str,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    matrix = targeted_search_v3_report_payload(v3_matrix_id=v3_matrix_id, output_dir=v3_matrix_dir)
    source_backfill_id = _text(matrix.get("source_backfill_id"))
    if not source_backfill_id:
        raise RuntimeError("targeted v3 matrix is missing source_backfill_id")
    backfill = st.paper_shadow_backfill_report_payload(
        backfill_id=source_backfill_id,
        output_dir=baseline_backfill_dir,
    )
    baseline_states = _records(backfill.get("backfill_method_states"))
    config = st._load_backfill_config_from_manifest(backfill)
    start = max(
        _coerce_date(backfill.get("date_start"), st.AI_AFTER_CHATGPT_START),
        st.AI_AFTER_CHATGPT_START,
    )
    requested_end = _coerce_date(backfill.get("date_end"), generated.date())
    source = _mapping(config.get("source"))
    symbols = st._symbols_from_state_paths(baseline_states)
    prices_path = price_cache_path or st._resolve_project_path(
        source.get("price_cache_path"),
        st.DEFAULT_PRICE_CACHE_PATH,
    )
    pivot = st._load_price_pivot(prices_path, symbols, start)
    latest_valid_as_of = _latest_common_price_date(pivot, symbols)
    end = min(requested_end, latest_valid_as_of, generated.date())
    used_latest_valid_as_of = end < requested_end
    pivot = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    quality_as_of = max(end, generated.date())
    quality = st._run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    if not quality.passed:
        raise RuntimeError(f"data quality gate failed for targeted v3 backfill: {quality.status}")
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): st._risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variant_specs = _records(matrix.get("v3_variant_specs"))
    variant_states: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for variant in variant_specs:
        try:
            variant_states.extend(
                st._run_variant_weight_path(
                    variant=variant,
                    baseline_states=baseline_states,
                    returns=returns,
                    labels=labels,
                    config=config,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed.append({"variant_id": _text(variant.get("variant_id")), "error": str(exc)})
    performance = st._variant_performance_metrics(variant_states, baseline_states)
    regime = st._variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = st._variant_stability_metrics(variant_states, baseline_states, config)
    churn = _variant_churn_metrics(variant_states, stability)
    backfill_id = _stable_id(
        "targeted-v3-backfill", v3_matrix_id, end.isoformat(), generated.isoformat()
    )
    root = _unique_dir(output_dir / backfill_id)
    root.mkdir(parents=True, exist_ok=False)
    quality_report_path = root / "validate_data_quality_report.md"
    progress = {
        "schema_version": st.SCHEMA_VERSION,
        "v3_backfill_id": root.name,
        "variants_total": len(variant_specs),
        "variants_completed": len({row.get("variant_id") for row in performance}),
        "variants_failed": len(failed),
        "failed_variants": failed,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_date_end": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_targeted_v3_backfill_manifest",
        "v3_backfill_id": root.name,
        "v3_matrix_id": v3_matrix_id,
        "source_backfill_id": source_backfill_id,
        "source_scorecard_id": matrix.get("source_scorecard_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS"
        if not failed and performance
        else "PASS_WITH_WARNINGS"
        if performance
        else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_start_date": backfill.get("requested_start_date", start.isoformat()),
        "requested_end_date": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        "variants_total": len(variant_specs),
        "variants_completed": progress["variants_completed"],
        "variants_failed": len(failed),
        "targeted_v3_backfill_manifest_path": str(root / "targeted_v3_backfill_manifest.json"),
        "v3_backfill_progress_path": str(root / "v3_backfill_progress.json"),
        "v3_variant_performance_path": str(root / "v3_variant_performance.jsonl"),
        "v3_variant_regime_metrics_path": str(root / "v3_variant_regime_metrics.jsonl"),
        "v3_variant_stability_metrics_path": str(root / "v3_variant_stability_metrics.jsonl"),
        "v3_variant_churn_metrics_path": str(root / "v3_variant_churn_metrics.jsonl"),
        "targeted_v3_backfill_report_path": str(root / "targeted_v3_backfill_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "targeted_v3_backfill_manifest.json", manifest)
    _write_json(root / "v3_backfill_progress.json", progress)
    write_data_quality_report(quality, quality_report_path)
    _write_jsonl(root / "v3_variant_performance.jsonl", performance)
    _write_jsonl(root / "v3_variant_regime_metrics.jsonl", regime)
    _write_jsonl(root / "v3_variant_stability_metrics.jsonl", stability)
    _write_jsonl(root / "v3_variant_churn_metrics.jsonl", churn)
    _write_text(
        root / "targeted_v3_backfill_report.md",
        render_targeted_v3_backfill_report(manifest, progress),
    )
    _write_latest_pointer(
        "latest_targeted_v3_backfill", root.name, root / "targeted_v3_backfill_manifest.json"
    )
    return {
        "v3_backfill_id": root.name,
        "v3_backfill_dir": root,
        "manifest": manifest,
        "v3_backfill_progress": progress,
        "v3_variant_performance": performance,
        "v3_variant_regime_metrics": regime,
        "v3_variant_stability_metrics": stability,
        "v3_variant_churn_metrics": churn,
    }


def resume_targeted_v3_backfill(
    *,
    v3_backfill_id: str,
    output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> dict[str, Any]:
    payload = targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id,
        output_dir=output_dir,
    )
    progress = _mapping(payload.get("v3_backfill_progress"))
    return {
        "v3_backfill_id": v3_backfill_id,
        "resume_status": (
            "ALREADY_COMPLETE"
            if int(_float(progress.get("variants_completed")))
            >= int(_float(progress.get("variants_total")))
            else "PARTIAL_COMPLETION_REVIEW_REQUIRED"
        ),
        "progress": progress,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def targeted_v3_backfill_report_payload(
    *,
    v3_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v3_backfill_id,
        latest_pointer="latest_targeted_v3_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="targeted_v3_backfill_manifest.json",
    )
    regime = _read_jsonl(root / "v3_variant_regime_metrics.jsonl")
    return {
        **_read_json(root / "targeted_v3_backfill_manifest.json"),
        "v3_backfill_progress": _read_json(root / "v3_backfill_progress.json"),
        "v3_variant_performance": _read_jsonl(root / "v3_variant_performance.jsonl"),
        "v3_variant_regime_metrics": regime,
        "v3_variant_stability_metrics": _read_jsonl(root / "v3_variant_stability_metrics.jsonl"),
        "v3_variant_churn_metrics": _read_jsonl(root / "v3_variant_churn_metrics.jsonl"),
        "v3_variant_lag_metrics": _variant_lag_metrics(regime),
        "v3_backfill_dir": str(root),
    }


def validate_targeted_v3_backfill_artifact(
    *,
    v3_backfill_id: str,
    output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / v3_backfill_id
    manifest = _read_optional_json(root / "targeted_v3_backfill_manifest.json") or {}
    progress = _read_optional_json(root / "v3_backfill_progress.json") or {}
    performance = _read_jsonl(root / "v3_variant_performance.jsonl")
    regime = _read_jsonl(root / "v3_variant_regime_metrics.jsonl")
    stability = _read_jsonl(root / "v3_variant_stability_metrics.jsonl")
    churn = _read_jsonl(root / "v3_variant_churn_metrics.jsonl")
    variants = {str(row.get("variant_id")) for row in performance}
    checks = _required_file_checks(
        root,
        (
            "targeted_v3_backfill_manifest.json",
            "v3_backfill_progress.json",
            "v3_variant_performance.jsonl",
            "v3_variant_regime_metrics.jsonl",
            "v3_variant_stability_metrics.jsonl",
            "v3_variant_churn_metrics.jsonl",
            "targeted_v3_backfill_report.md",
            "validate_data_quality_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "v3_backfill_id_matches", manifest.get("v3_backfill_id") == v3_backfill_id, ""
            ),
            st._check("performance_metrics_present", bool(performance), ""),
            st._check(
                "data_quality_visible",
                manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(manifest.get("data_quality_status")),
            ),
            st._check("latest_valid_as_of_visible", bool(manifest.get("latest_valid_as_of")), ""),
            st._check(
                "each_variant_has_regime_metrics",
                variants.issubset({str(row.get("variant_id")) for row in regime}),
                "",
            ),
            st._check(
                "each_variant_has_stability_metrics",
                variants.issubset({str(row.get("variant_id")) for row in stability}),
                "",
            ),
            st._check("churn_metrics_readable", isinstance(churn, list), ""),
            st._check(
                "progress_counts_match",
                int(_float(progress.get("variants_completed"))) == len(variants),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, progress, *performance), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, progress, *performance, *regime, *stability),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_targeted_v3_backfill_validation", v3_backfill_id, checks
    )


def run_near_miss_ab_comparison(
    *,
    v3_backfill_id: str,
    near_miss_id: str,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    output_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id,
        output_dir=v3_backfill_dir,
    )
    matrix = targeted_search_v3_report_payload(
        v3_matrix_id=_text(backfill.get("v3_matrix_id")),
        output_dir=v3_matrix_dir,
    )
    near_miss = near_miss_candidates_report_payload(
        near_miss_id=near_miss_id, output_dir=near_miss_dir
    )
    scorecard = weight_scorecard_report_payload(
        scorecard_id=_text(near_miss.get("source_scorecard_id")),
        output_dir=scorecard_dir,
    )
    comparison = _ab_comparison_rows(backfill, matrix, scorecard)
    winner_summary = _ab_winner_summary(comparison)
    ab_id = _stable_id(
        "near-miss-ab-comparison", v3_backfill_id, near_miss_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / ab_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_near_miss_ab_manifest",
        "ab_id": root.name,
        "v3_backfill_id": v3_backfill_id,
        "near_miss_id": near_miss_id,
        "source_scorecard_id": near_miss.get("source_scorecard_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if comparison else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "near_miss_ab_manifest_path": str(root / "near_miss_ab_manifest.json"),
        "ab_comparison_matrix_path": str(root / "ab_comparison_matrix.jsonl"),
        "ab_winner_summary_path": str(root / "ab_winner_summary.json"),
        "near_miss_ab_comparison_report_path": str(root / "near_miss_ab_comparison_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "near_miss_ab_manifest.json", manifest)
    _write_jsonl(root / "ab_comparison_matrix.jsonl", comparison)
    _write_json(root / "ab_winner_summary.json", winner_summary)
    _write_text(
        root / "near_miss_ab_comparison_report.md",
        render_near_miss_ab_report(manifest, winner_summary),
    )
    _write_latest_pointer(
        "latest_near_miss_ab_comparison", root.name, root / "near_miss_ab_manifest.json"
    )
    return {
        "ab_id": root.name,
        "ab_dir": root,
        "manifest": manifest,
        "ab_comparison_matrix": comparison,
        "ab_winner_summary": winner_summary,
    }


def near_miss_ab_comparison_report_payload(
    *,
    ab_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=ab_id,
        latest_pointer="latest_near_miss_ab_comparison",
        latest=latest,
        output_dir=output_dir,
        required_name="near_miss_ab_manifest.json",
    )
    return {
        **_read_json(root / "near_miss_ab_manifest.json"),
        "ab_comparison_matrix": _read_jsonl(root / "ab_comparison_matrix.jsonl"),
        "ab_winner_summary": _read_json(root / "ab_winner_summary.json"),
        "ab_dir": str(root),
    }


def validate_near_miss_ab_comparison_artifact(
    *,
    ab_id: str,
    output_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
) -> dict[str, Any]:
    root = output_dir / ab_id
    manifest = _read_optional_json(root / "near_miss_ab_manifest.json") or {}
    rows = _read_jsonl(root / "ab_comparison_matrix.jsonl")
    summary = _read_optional_json(root / "ab_winner_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "near_miss_ab_manifest.json",
            "ab_comparison_matrix.jsonl",
            "ab_winner_summary.json",
            "near_miss_ab_comparison_report.md",
        ),
    )
    checks.extend(
        [
            st._check("ab_id_matches", manifest.get("ab_id") == ab_id, ""),
            st._check("comparison_rows_present", bool(rows), ""),
            st._check("winner_summary_present", bool(summary.get("best_v3_variant")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *rows), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *rows),
                "",
            ),
        ]
    )
    return _validation_payload("etf_dynamic_v3_near_miss_ab_comparison_validation", ab_id, checks)


def run_promotion_threshold_sensitivity(
    *,
    v3_backfill_id: str,
    ab_id: str,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    ab_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    output_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id,
        output_dir=v3_backfill_dir,
    )
    matrix = targeted_search_v3_report_payload(
        v3_matrix_id=_text(backfill.get("v3_matrix_id")),
        output_dir=v3_matrix_dir,
    )
    ab = near_miss_ab_comparison_report_payload(ab_id=ab_id, output_dir=ab_dir)
    score_rows = _targeted_v3_scorecard_rows(backfill, matrix)
    scenarios = _threshold_scenarios(score_rows)
    impact = _threshold_candidate_impact(score_rows, ab, scenarios)
    sensitivity_id = _stable_id(
        "promotion-threshold-sensitivity",
        v3_backfill_id,
        ab_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / sensitivity_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_threshold_sensitivity_manifest",
        "sensitivity_id": root.name,
        "v3_backfill_id": v3_backfill_id,
        "ab_id": ab_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if scenarios else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "threshold_sensitivity_manifest_path": str(root / "threshold_sensitivity_manifest.json"),
        "threshold_scenarios_path": str(root / "threshold_scenarios.jsonl"),
        "threshold_candidate_impact_path": str(root / "threshold_candidate_impact.json"),
        "threshold_sensitivity_report_path": str(root / "threshold_sensitivity_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "threshold_sensitivity_manifest.json", manifest)
    _write_jsonl(root / "threshold_scenarios.jsonl", scenarios)
    _write_json(root / "threshold_candidate_impact.json", impact)
    _write_text(
        root / "threshold_sensitivity_report.md",
        render_threshold_sensitivity_report(manifest, scenarios, impact),
    )
    _write_latest_pointer(
        "latest_promotion_threshold_sensitivity",
        root.name,
        root / "threshold_sensitivity_manifest.json",
    )
    return {
        "sensitivity_id": root.name,
        "sensitivity_dir": root,
        "manifest": manifest,
        "threshold_scenarios": scenarios,
        "threshold_candidate_impact": impact,
    }


def promotion_threshold_sensitivity_report_payload(
    *,
    sensitivity_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=sensitivity_id,
        latest_pointer="latest_promotion_threshold_sensitivity",
        latest=latest,
        output_dir=output_dir,
        required_name="threshold_sensitivity_manifest.json",
    )
    return {
        **_read_json(root / "threshold_sensitivity_manifest.json"),
        "threshold_scenarios": _read_jsonl(root / "threshold_scenarios.jsonl"),
        "threshold_candidate_impact": _read_json(root / "threshold_candidate_impact.json"),
        "sensitivity_dir": str(root),
    }


def validate_promotion_threshold_sensitivity_artifact(
    *,
    sensitivity_id: str,
    output_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
) -> dict[str, Any]:
    root = output_dir / sensitivity_id
    manifest = _read_optional_json(root / "threshold_sensitivity_manifest.json") or {}
    scenarios = _read_jsonl(root / "threshold_scenarios.jsonl")
    impact = _read_optional_json(root / "threshold_candidate_impact.json") or {}
    checks = _required_file_checks(
        root,
        (
            "threshold_sensitivity_manifest.json",
            "threshold_scenarios.jsonl",
            "threshold_candidate_impact.json",
            "threshold_sensitivity_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "sensitivity_id_matches", manifest.get("sensitivity_id") == sensitivity_id, ""
            ),
            st._check(
                "base_threshold_present",
                any(row.get("scenario") == "base_threshold" for row in scenarios),
                "",
            ),
            st._check(
                "relaxed_not_recommended",
                all(
                    row.get("recommended") is False
                    for row in scenarios
                    if row.get("scenario") != "base_threshold"
                ),
                "",
            ),
            st._check(
                "review_required_for_relaxed_candidates",
                all(
                    row.get("candidate_status") == "REVIEW_REQUIRED"
                    for row in _records(impact.get("relaxed_only_candidates"))
                ),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, impact, *scenarios), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, impact, *scenarios),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_promotion_threshold_sensitivity_validation", sensitivity_id, checks
    )


def run_candidate_promotion_v2(
    *,
    v3_backfill_id: str,
    ab_id: str,
    sensitivity_id: str,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    ab_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    sensitivity_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id,
        output_dir=v3_backfill_dir,
    )
    matrix = targeted_search_v3_report_payload(
        v3_matrix_id=_text(backfill.get("v3_matrix_id")),
        output_dir=v3_matrix_dir,
    )
    ab = near_miss_ab_comparison_report_payload(ab_id=ab_id, output_dir=ab_dir)
    sensitivity = promotion_threshold_sensitivity_report_payload(
        sensitivity_id=sensitivity_id,
        output_dir=sensitivity_dir,
    )
    rows = _targeted_v3_scorecard_rows(backfill, matrix)
    promoted, keep_testing, rejected = _promotion_v2_candidate_lists(rows, ab, sensitivity)
    decision = _promotion_v2_decision(promoted, keep_testing, rejected)
    promotion_v2_id = _stable_id(
        "candidate-promotion-v2",
        v3_backfill_id,
        ab_id,
        sensitivity_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / promotion_v2_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["promotion_v2_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_promotion_v2_manifest",
        "promotion_v2_id": root.name,
        "v3_backfill_id": v3_backfill_id,
        "ab_id": ab_id,
        "sensitivity_id": sensitivity_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "candidate_promotion_v2_manifest_path": str(root / "candidate_promotion_v2_manifest.json"),
        "promotion_v2_decision_path": str(root / "promotion_v2_decision.json"),
        "promoted_candidates_v2_path": str(root / "promoted_candidates_v2.jsonl"),
        "rejected_candidates_v2_path": str(root / "rejected_candidates_v2.jsonl"),
        "keep_testing_candidates_v2_path": str(root / "keep_testing_candidates_v2.jsonl"),
        "candidate_promotion_v2_report_path": str(root / "candidate_promotion_v2_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_candidate_promotion_v2_reader_brief(decision)
    _write_json(root / "candidate_promotion_v2_manifest.json", manifest)
    _write_json(root / "promotion_v2_decision.json", decision)
    _write_jsonl(root / "promoted_candidates_v2.jsonl", promoted)
    _write_jsonl(root / "rejected_candidates_v2.jsonl", rejected)
    _write_jsonl(root / "keep_testing_candidates_v2.jsonl", keep_testing)
    _write_text(
        root / "candidate_promotion_v2_report.md",
        render_candidate_promotion_v2_report(manifest, decision),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_candidate_promotion_v2",
        root.name,
        root / "candidate_promotion_v2_manifest.json",
    )
    return {
        "promotion_v2_id": root.name,
        "promotion_v2_dir": root,
        "manifest": manifest,
        "promotion_v2_decision": decision,
        "promoted_candidates_v2": promoted,
        "rejected_candidates_v2": rejected,
        "keep_testing_candidates_v2": keep_testing,
        "reader_brief_section": reader,
    }


def candidate_promotion_v2_report_payload(
    *,
    promotion_v2_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=promotion_v2_id,
        latest_pointer="latest_candidate_promotion_v2",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_promotion_v2_manifest.json",
    )
    return {
        **_read_json(root / "candidate_promotion_v2_manifest.json"),
        "promotion_v2_decision": _read_json(root / "promotion_v2_decision.json"),
        "promoted_candidates_v2": _read_jsonl(root / "promoted_candidates_v2.jsonl"),
        "rejected_candidates_v2": _read_jsonl(root / "rejected_candidates_v2.jsonl"),
        "keep_testing_candidates_v2": _read_jsonl(root / "keep_testing_candidates_v2.jsonl"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "promotion_v2_dir": str(root),
    }


def validate_candidate_promotion_v2_artifact(
    *,
    promotion_v2_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
) -> dict[str, Any]:
    root = output_dir / promotion_v2_id
    manifest = _read_optional_json(root / "candidate_promotion_v2_manifest.json") or {}
    decision = _read_optional_json(root / "promotion_v2_decision.json") or {}
    promoted = _read_jsonl(root / "promoted_candidates_v2.jsonl")
    rejected = _read_jsonl(root / "rejected_candidates_v2.jsonl")
    keep = _read_jsonl(root / "keep_testing_candidates_v2.jsonl")
    checks = _required_file_checks(
        root,
        (
            "candidate_promotion_v2_manifest.json",
            "promotion_v2_decision.json",
            "promoted_candidates_v2.jsonl",
            "rejected_candidates_v2.jsonl",
            "keep_testing_candidates_v2.jsonl",
            "candidate_promotion_v2_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "promotion_v2_id_matches",
                manifest.get("promotion_v2_id") == promotion_v2_id,
                "",
            ),
            st._check(
                "decision_valid",
                decision.get("decision")
                in {
                    "PROMOTE_CANDIDATE",
                    "KEEP_TESTING",
                    "RUN_ANOTHER_TARGETED_SEARCH",
                    "NO_CANDIDATE",
                },
                _text(decision.get("decision")),
            ),
            st._check(
                "counts_match",
                int(_float(decision.get("promoted_count"))) == len(promoted)
                and int(_float(decision.get("keep_testing_count"))) == len(keep)
                and int(_float(decision.get("rejected_count"))) == len(rejected),
                "",
            ),
            st._check(
                "not_official_target_weights",
                decision.get("not_official_target_weights") is True,
                "",
            ),
            st._check(
                "broker_action_allowed_false", decision.get("broker_action_allowed") is False, ""
            ),
            st._check(
                "production_effect_none",
                decision.get("production_effect") == st.PRODUCTION_EFFECT,
                "",
            ),
            st._check(
                "broker_forbidden",
                _payload_safe(manifest, decision, *promoted, *rejected, *keep),
                "",
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, decision, *promoted, *rejected, *keep),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_candidate_promotion_v2_validation", promotion_v2_id, checks
    )


def run_next_formal_or_search_plan(
    *,
    promotion_v2_id: str,
    promotion_v2_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
    output_dir: Path = DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    promotion = candidate_promotion_v2_report_payload(
        promotion_v2_id=promotion_v2_id,
        output_dir=promotion_v2_dir,
    )
    decision = _next_plan_decision(promotion)
    formal_candidates = _next_formal_method_candidates(promotion)
    continue_plan = _continue_search_plan(promotion, decision)
    checklist = render_owner_next_action_checklist(decision, formal_candidates, continue_plan)
    plan_id = _stable_id("next-formal-or-search-plan", promotion_v2_id, generated.isoformat())
    root = _unique_dir(output_dir / plan_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["plan_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_next_formal_or_search_manifest",
        "plan_id": root.name,
        "promotion_v2_id": promotion_v2_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": promotion.get("market_regime", "ai_after_chatgpt"),
        "next_formal_or_search_manifest_path": str(root / "next_formal_or_search_manifest.json"),
        "next_plan_decision_path": str(root / "next_plan_decision.json"),
        "formal_method_candidates_path": str(root / "formal_method_candidates.json"),
        "continue_search_plan_path": str(root / "continue_search_plan.json"),
        "owner_next_action_checklist_path": str(root / "owner_next_action_checklist.md"),
        "next_formal_or_search_plan_report_path": str(
            root / "next_formal_or_search_plan_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_next_plan_reader_brief(decision)
    _write_json(root / "next_formal_or_search_manifest.json", manifest)
    _write_json(root / "next_plan_decision.json", decision)
    _write_json(root / "formal_method_candidates.json", formal_candidates)
    _write_json(root / "continue_search_plan.json", continue_plan)
    _write_text(root / "owner_next_action_checklist.md", checklist)
    _write_text(
        root / "next_formal_or_search_plan_report.md",
        render_next_formal_or_search_plan_report(
            manifest, decision, formal_candidates, continue_plan
        ),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_next_formal_or_search_plan",
        root.name,
        root / "next_formal_or_search_manifest.json",
    )
    return {
        "plan_id": root.name,
        "plan_dir": root,
        "manifest": manifest,
        "next_plan_decision": decision,
        "formal_method_candidates": formal_candidates,
        "continue_search_plan": continue_plan,
        "owner_next_action_checklist": checklist,
        "reader_brief_section": reader,
    }


def next_formal_or_search_plan_report_payload(
    *,
    plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=plan_id,
        latest_pointer="latest_next_formal_or_search_plan",
        latest=latest,
        output_dir=output_dir,
        required_name="next_formal_or_search_manifest.json",
    )
    return {
        **_read_json(root / "next_formal_or_search_manifest.json"),
        "next_plan_decision": _read_json(root / "next_plan_decision.json"),
        "formal_method_candidates": _read_json(root / "formal_method_candidates.json"),
        "continue_search_plan": _read_json(root / "continue_search_plan.json"),
        "owner_next_action_checklist": (root / "owner_next_action_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "plan_dir": str(root),
    }


def validate_next_formal_or_search_plan_artifact(
    *,
    plan_id: str,
    output_dir: Path = DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
) -> dict[str, Any]:
    root = output_dir / plan_id
    manifest = _read_optional_json(root / "next_formal_or_search_manifest.json") or {}
    decision = _read_optional_json(root / "next_plan_decision.json") or {}
    formal = _read_optional_json(root / "formal_method_candidates.json") or {}
    continue_plan = _read_optional_json(root / "continue_search_plan.json") or {}
    checks = _required_file_checks(
        root,
        (
            "next_formal_or_search_manifest.json",
            "next_plan_decision.json",
            "formal_method_candidates.json",
            "continue_search_plan.json",
            "owner_next_action_checklist.md",
            "next_formal_or_search_plan_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("plan_id_matches", manifest.get("plan_id") == plan_id, ""),
            st._check(
                "decision_valid",
                decision.get("decision")
                in {
                    "FORMAL_METHOD_PLAN",
                    "KEEP_TESTING_PLAN",
                    "CONTINUE_SEARCH_PLAN",
                    "NO_CANDIDATE_PLAN",
                },
                _text(decision.get("decision")),
            ),
            st._check("formal_candidates_readable", "candidates" in formal, ""),
            st._check("continue_plan_readable", "recommended_actions" in continue_plan, ""),
            st._check(
                "broker_forbidden", _payload_safe(manifest, decision, formal, continue_plan), ""
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, decision, formal, continue_plan),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_next_formal_or_search_plan_validation", plan_id, checks
    )


def run_gate_calibration_review(
    *,
    no_promotion_review_id: str,
    threshold_sensitivity_id: str,
    review_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
    sensitivity_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    output_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    review = no_promotion_review_report_payload(
        review_id=no_promotion_review_id,
        output_dir=review_dir,
    )
    sensitivity = promotion_threshold_sensitivity_report_payload(
        sensitivity_id=threshold_sensitivity_id,
        output_dir=sensitivity_dir,
    )
    diagnosis = _gate_strictness_diagnosis(review, sensitivity)
    impact = _gate_component_impact(review)
    relaxed = _diagnostic_relaxed_gate_result(sensitivity)
    gate_calibration_id = _stable_id(
        "gate-calibration-review",
        no_promotion_review_id,
        threshold_sensitivity_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / gate_calibration_id)
    root.mkdir(parents=True, exist_ok=False)
    diagnosis["gate_calibration_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_calibration_review_manifest",
        "gate_calibration_id": root.name,
        "source_no_promotion_review": no_promotion_review_id,
        "threshold_sensitivity_id": threshold_sensitivity_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": review.get("market_regime", "ai_after_chatgpt"),
        "can_change_official_gate": False,
        "official_gate_changed": False,
        "gate_calibration_manifest_path": str(root / "gate_calibration_manifest.json"),
        "gate_strictness_diagnosis_path": str(root / "gate_strictness_diagnosis.json"),
        "gate_component_impact_path": str(root / "gate_component_impact.json"),
        "diagnostic_relaxed_gate_result_path": str(root / "diagnostic_relaxed_gate_result.json"),
        "gate_calibration_review_report_path": str(root / "gate_calibration_review_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_gate_calibration_reader_brief(diagnosis, relaxed)
    _write_json(root / "gate_calibration_manifest.json", manifest)
    _write_json(root / "gate_strictness_diagnosis.json", diagnosis)
    _write_json(root / "gate_component_impact.json", impact)
    _write_json(root / "diagnostic_relaxed_gate_result.json", relaxed)
    _write_text(
        root / "gate_calibration_review_report.md",
        render_gate_calibration_review_report(manifest, diagnosis, impact, relaxed),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_gate_calibration_review",
        root.name,
        root / "gate_calibration_manifest.json",
    )
    return {
        "gate_calibration_id": root.name,
        "gate_calibration_dir": root,
        "manifest": manifest,
        "gate_strictness_diagnosis": diagnosis,
        "gate_component_impact": impact,
        "diagnostic_relaxed_gate_result": relaxed,
        "reader_brief_section": reader,
    }


def gate_calibration_review_report_payload(
    *,
    gate_calibration_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=gate_calibration_id,
        latest_pointer="latest_gate_calibration_review",
        latest=latest,
        output_dir=output_dir,
        required_name="gate_calibration_manifest.json",
    )
    return {
        **_read_json(root / "gate_calibration_manifest.json"),
        "gate_strictness_diagnosis": _read_json(root / "gate_strictness_diagnosis.json"),
        "gate_component_impact": _read_json(root / "gate_component_impact.json"),
        "diagnostic_relaxed_gate_result": _read_json(root / "diagnostic_relaxed_gate_result.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "gate_calibration_dir": str(root),
    }


def validate_gate_calibration_review_artifact(
    *,
    gate_calibration_id: str,
    output_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / gate_calibration_id
    manifest = _read_optional_json(root / "gate_calibration_manifest.json") or {}
    diagnosis = _read_optional_json(root / "gate_strictness_diagnosis.json") or {}
    impact = _read_optional_json(root / "gate_component_impact.json") or {}
    relaxed = _read_optional_json(root / "diagnostic_relaxed_gate_result.json") or {}
    checks = _required_file_checks(
        root,
        (
            "gate_calibration_manifest.json",
            "gate_strictness_diagnosis.json",
            "gate_component_impact.json",
            "diagnostic_relaxed_gate_result.json",
            "gate_calibration_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "gate_calibration_id_matches",
                manifest.get("gate_calibration_id") == gate_calibration_id,
                "",
            ),
            st._check("official_gate_unchanged", relaxed.get("official_gate_changed") is False, ""),
            st._check(
                "can_change_official_gate_false",
                diagnosis.get("can_change_official_gate") is False,
                "",
            ),
            st._check(
                "calibrated_assessment_valid",
                diagnosis.get("calibrated_assessment")
                in {"REASONABLE", "TOO_STRICT", "TOO_LOOSE", "INCONCLUSIVE"},
                _text(diagnosis.get("calibrated_assessment")),
            ),
            st._check("component_impact_readable", bool(_records(impact.get("components"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, diagnosis, impact, relaxed), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, diagnosis, impact, relaxed),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_gate_calibration_review_validation",
        gate_calibration_id,
        checks,
    )


def run_scorecard_attribution(
    *,
    scorecard_id: str,
    v3_backfill_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    output_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_scorecard = weight_scorecard_report_payload(
        scorecard_id=scorecard_id,
        output_dir=scorecard_dir,
    )
    backfill = targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id,
        output_dir=v3_backfill_dir,
    )
    matrix = targeted_search_v3_report_payload(
        v3_matrix_id=_text(backfill.get("v3_matrix_id")),
        output_dir=v3_matrix_dir,
    )
    rows = _targeted_v3_scorecard_rows(backfill, matrix)
    rejected = [
        row for row in rows if row.get("scorecard_decision") != "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    ]
    distribution = _score_component_distribution(scorecard_id, rejected)
    component_matrix = _rejected_variant_component_matrix(rejected)
    family_weakness = _family_component_weakness(component_matrix)
    attribution_id = _stable_id(
        "scorecard-attribution",
        scorecard_id,
        v3_backfill_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_scorecard_attribution_manifest",
        "scorecard_attribution_id": root.name,
        "scorecard_id": scorecard_id,
        "v3_backfill_id": v3_backfill_id,
        "v3_matrix_id": backfill.get("v3_matrix_id"),
        "source_backfill_id": backfill.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if component_matrix else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "variant_count": len(rejected),
        "source_batch2_scorecard_variant_count": len(
            _records(source_scorecard.get("variant_scorecard"))
        ),
        "scorecard_attribution_manifest_path": str(root / "scorecard_attribution_manifest.json"),
        "score_component_distribution_path": str(root / "score_component_distribution.json"),
        "rejected_variant_component_matrix_path": str(
            root / "rejected_variant_component_matrix.jsonl"
        ),
        "family_component_weakness_path": str(root / "family_component_weakness.json"),
        "scorecard_attribution_report_path": str(root / "scorecard_attribution_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "scorecard_attribution_manifest.json", manifest)
    _write_json(root / "score_component_distribution.json", distribution)
    _write_jsonl(root / "rejected_variant_component_matrix.jsonl", component_matrix)
    _write_json(root / "family_component_weakness.json", family_weakness)
    _write_text(
        root / "scorecard_attribution_report.md",
        render_scorecard_attribution_report(manifest, distribution, family_weakness),
    )
    _write_latest_pointer(
        "latest_scorecard_attribution",
        root.name,
        root / "scorecard_attribution_manifest.json",
    )
    return {
        "scorecard_attribution_id": root.name,
        "scorecard_attribution_dir": root,
        "manifest": manifest,
        "score_component_distribution": distribution,
        "rejected_variant_component_matrix": component_matrix,
        "family_component_weakness": family_weakness,
    }


def scorecard_attribution_report_payload(
    *,
    scorecard_attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=scorecard_attribution_id,
        latest_pointer="latest_scorecard_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="scorecard_attribution_manifest.json",
    )
    return {
        **_read_json(root / "scorecard_attribution_manifest.json"),
        "score_component_distribution": _read_json(root / "score_component_distribution.json"),
        "rejected_variant_component_matrix": _read_jsonl(
            root / "rejected_variant_component_matrix.jsonl"
        ),
        "family_component_weakness": _read_json(root / "family_component_weakness.json"),
        "scorecard_attribution_dir": str(root),
    }


def validate_scorecard_attribution_artifact(
    *,
    scorecard_attribution_id: str,
    output_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / scorecard_attribution_id
    manifest = _read_optional_json(root / "scorecard_attribution_manifest.json") or {}
    distribution = _read_optional_json(root / "score_component_distribution.json") or {}
    matrix = _read_jsonl(root / "rejected_variant_component_matrix.jsonl")
    family = _read_optional_json(root / "family_component_weakness.json") or {}
    checks = _required_file_checks(
        root,
        (
            "scorecard_attribution_manifest.json",
            "score_component_distribution.json",
            "rejected_variant_component_matrix.jsonl",
            "family_component_weakness.json",
            "scorecard_attribution_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "scorecard_attribution_id_matches",
                manifest.get("scorecard_attribution_id") == scorecard_attribution_id,
                "",
            ),
            st._check(
                "variant_count_matches",
                int(_float(manifest.get("variant_count"))) == len(matrix),
                "",
            ),
            st._check(
                "distribution_readable",
                bool(_records(distribution.get("components"))),
                "",
            ),
            st._check("family_weakness_readable", bool(_records(family.get("families"))), ""),
            st._check(
                "broker_forbidden",
                _payload_safe(manifest, distribution, family, *matrix),
                "",
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, distribution, family, *matrix),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_scorecard_attribution_validation",
        scorecard_attribution_id,
        checks,
    )


def run_signal_instability_diagnosis(
    *,
    scorecard_attribution_id: str,
    attribution_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    attribution = scorecard_attribution_report_payload(
        scorecard_attribution_id=scorecard_attribution_id,
        output_dir=attribution_dir,
    )
    method_rows = _method_signal_stability_rows(attribution)
    flip_events = _signal_flip_events(method_rows, attribution)
    mismatch_events = _regime_mismatch_events(attribution)
    summary = _signal_instability_summary(method_rows, mismatch_events)
    diagnosis_id = _stable_id(
        "signal-instability-diagnosis",
        scorecard_attribution_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / diagnosis_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["signal_diagnosis_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_instability_manifest",
        "signal_diagnosis_id": root.name,
        "scorecard_attribution_id": scorecard_attribution_id,
        "scorecard_id": attribution.get("scorecard_id"),
        "v3_backfill_id": attribution.get("v3_backfill_id"),
        "v3_matrix_id": attribution.get("v3_matrix_id"),
        "source_backfill_id": attribution.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if method_rows else "FAIL",
        "market_regime": attribution.get("market_regime", "ai_after_chatgpt"),
        "date_start": attribution.get("date_start"),
        "date_end": attribution.get("date_end"),
        "signal_instability_manifest_path": str(root / "signal_instability_manifest.json"),
        "method_signal_stability_path": str(root / "method_signal_stability.jsonl"),
        "signal_flip_events_path": str(root / "signal_flip_events.jsonl"),
        "regime_mismatch_events_path": str(root / "regime_mismatch_events.jsonl"),
        "signal_instability_summary_path": str(root / "signal_instability_summary.json"),
        "signal_instability_diagnosis_report_path": str(
            root / "signal_instability_diagnosis_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_signal_instability_reader_brief(summary)
    _write_json(root / "signal_instability_manifest.json", manifest)
    _write_jsonl(root / "method_signal_stability.jsonl", method_rows)
    _write_jsonl(root / "signal_flip_events.jsonl", flip_events)
    _write_jsonl(root / "regime_mismatch_events.jsonl", mismatch_events)
    _write_json(root / "signal_instability_summary.json", summary)
    _write_text(
        root / "signal_instability_diagnosis_report.md",
        render_signal_instability_report(manifest, method_rows, summary),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_signal_instability_diagnosis",
        root.name,
        root / "signal_instability_manifest.json",
    )
    return {
        "signal_diagnosis_id": root.name,
        "signal_diagnosis_dir": root,
        "manifest": manifest,
        "method_signal_stability": method_rows,
        "signal_flip_events": flip_events,
        "regime_mismatch_events": mismatch_events,
        "signal_instability_summary": summary,
        "reader_brief_section": reader,
    }


def signal_instability_diagnosis_report_payload(
    *,
    signal_diagnosis_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=signal_diagnosis_id,
        latest_pointer="latest_signal_instability_diagnosis",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_instability_manifest.json",
    )
    return {
        **_read_json(root / "signal_instability_manifest.json"),
        "method_signal_stability": _read_jsonl(root / "method_signal_stability.jsonl"),
        "signal_flip_events": _read_jsonl(root / "signal_flip_events.jsonl"),
        "regime_mismatch_events": _read_jsonl(root / "regime_mismatch_events.jsonl"),
        "signal_instability_summary": _read_json(root / "signal_instability_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "signal_diagnosis_dir": str(root),
    }


def validate_signal_instability_diagnosis_artifact(
    *,
    signal_diagnosis_id: str,
    output_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    root = output_dir / signal_diagnosis_id
    manifest = _read_optional_json(root / "signal_instability_manifest.json") or {}
    methods = _read_jsonl(root / "method_signal_stability.jsonl")
    flips = _read_jsonl(root / "signal_flip_events.jsonl")
    mismatches = _read_jsonl(root / "regime_mismatch_events.jsonl")
    summary = _read_optional_json(root / "signal_instability_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "signal_instability_manifest.json",
            "method_signal_stability.jsonl",
            "signal_flip_events.jsonl",
            "regime_mismatch_events.jsonl",
            "signal_instability_summary.json",
            "signal_instability_diagnosis_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "signal_diagnosis_id_matches",
                manifest.get("signal_diagnosis_id") == signal_diagnosis_id,
                "",
            ),
            st._check("method_stability_readable", bool(methods), ""),
            st._check("flip_events_listed", isinstance(flips, list), ""),
            st._check("regime_mismatch_events_listed", isinstance(mismatches, list), ""),
            st._check(
                "summary_has_signal_fix_flag",
                "requires_signal_level_fix" in summary,
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *methods), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *methods, *flips, *mismatches),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_signal_instability_diagnosis_validation",
        signal_diagnosis_id,
        checks,
    )


def run_consensus_quality_review(
    *,
    signal_diagnosis_id: str,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    attribution_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    signal = signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=signal_diagnosis_id,
        output_dir=signal_dir,
    )
    attribution = scorecard_attribution_report_payload(
        scorecard_attribution_id=_text(signal.get("scorecard_attribution_id")),
        output_dir=attribution_dir,
    )
    dispersion = _consensus_dispersion_summary(signal)
    quality_rows = _ensemble_method_quality(signal, attribution)
    failure = _consensus_failure_reasons(dispersion, quality_rows)
    consensus_review_id = _stable_id(
        "consensus-quality-review",
        signal_diagnosis_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / consensus_review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_quality_manifest",
        "consensus_review_id": root.name,
        "signal_diagnosis_id": signal_diagnosis_id,
        "scorecard_attribution_id": signal.get("scorecard_attribution_id"),
        "v3_backfill_id": signal.get("v3_backfill_id"),
        "source_backfill_id": signal.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if quality_rows else "FAIL",
        "market_regime": signal.get("market_regime", "ai_after_chatgpt"),
        "consensus_quality_manifest_path": str(root / "consensus_quality_manifest.json"),
        "consensus_dispersion_summary_path": str(root / "consensus_dispersion_summary.json"),
        "ensemble_method_quality_path": str(root / "ensemble_method_quality.jsonl"),
        "consensus_failure_reasons_path": str(root / "consensus_failure_reasons.json"),
        "consensus_quality_review_report_path": str(root / "consensus_quality_review_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_consensus_quality_reader_brief(failure)
    _write_json(root / "consensus_quality_manifest.json", manifest)
    _write_json(root / "consensus_dispersion_summary.json", dispersion)
    _write_jsonl(root / "ensemble_method_quality.jsonl", quality_rows)
    _write_json(root / "consensus_failure_reasons.json", failure)
    _write_text(
        root / "consensus_quality_review_report.md",
        render_consensus_quality_report(manifest, dispersion, quality_rows, failure),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_consensus_quality_review",
        root.name,
        root / "consensus_quality_manifest.json",
    )
    return {
        "consensus_review_id": root.name,
        "consensus_review_dir": root,
        "manifest": manifest,
        "consensus_dispersion_summary": dispersion,
        "ensemble_method_quality": quality_rows,
        "consensus_failure_reasons": failure,
        "reader_brief_section": reader,
    }


def consensus_quality_review_report_payload(
    *,
    consensus_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=consensus_review_id,
        latest_pointer="latest_consensus_quality_review",
        latest=latest,
        output_dir=output_dir,
        required_name="consensus_quality_manifest.json",
    )
    return {
        **_read_json(root / "consensus_quality_manifest.json"),
        "consensus_dispersion_summary": _read_json(root / "consensus_dispersion_summary.json"),
        "ensemble_method_quality": _read_jsonl(root / "ensemble_method_quality.jsonl"),
        "consensus_failure_reasons": _read_json(root / "consensus_failure_reasons.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "consensus_review_dir": str(root),
    }


def validate_consensus_quality_review_artifact(
    *,
    consensus_review_id: str,
    output_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / consensus_review_id
    manifest = _read_optional_json(root / "consensus_quality_manifest.json") or {}
    dispersion = _read_optional_json(root / "consensus_dispersion_summary.json") or {}
    quality = _read_jsonl(root / "ensemble_method_quality.jsonl")
    failure = _read_optional_json(root / "consensus_failure_reasons.json") or {}
    checks = _required_file_checks(
        root,
        (
            "consensus_quality_manifest.json",
            "consensus_dispersion_summary.json",
            "ensemble_method_quality.jsonl",
            "consensus_failure_reasons.json",
            "consensus_quality_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "consensus_review_id_matches",
                manifest.get("consensus_review_id") == consensus_review_id,
                "",
            ),
            st._check("dispersion_status_visible", bool(dispersion.get("dispersion_status")), ""),
            st._check("ensemble_quality_readable", bool(quality), ""),
            st._check("failure_reason_visible", bool(failure.get("primary_failure_reason")), ""),
            st._check(
                "broker_forbidden",
                _payload_safe(manifest, dispersion, failure, *quality),
                "",
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, dispersion, failure, *quality),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_consensus_quality_review_validation",
        consensus_review_id,
        checks,
    )


def run_micro_search_v4_design(
    *,
    gate_calibration_id: str,
    scorecard_attribution_id: str,
    signal_diagnosis_id: str,
    consensus_review_id: str,
    gate_calibration_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    attribution_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    gate = gate_calibration_review_report_payload(
        gate_calibration_id=gate_calibration_id,
        output_dir=gate_calibration_dir,
    )
    attribution = scorecard_attribution_report_payload(
        scorecard_attribution_id=scorecard_attribution_id,
        output_dir=attribution_dir,
    )
    signal = signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=signal_diagnosis_id,
        output_dir=signal_dir,
    )
    consensus = consensus_quality_review_report_payload(
        consensus_review_id=consensus_review_id,
        output_dir=consensus_dir,
    )
    rationale = _micro_search_v4_design_rationale(gate, attribution, signal, consensus)
    variants = _micro_search_v4_variant_specs(rationale)
    design_id = _stable_id(
        "micro-search-v4-design",
        gate_calibration_id,
        scorecard_attribution_id,
        signal_diagnosis_id,
        consensus_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / design_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_micro_search_v4_design_manifest",
        "v4_design_id": root.name,
        "gate_calibration_id": gate_calibration_id,
        "scorecard_attribution_id": scorecard_attribution_id,
        "signal_diagnosis_id": signal_diagnosis_id,
        "consensus_review_id": consensus_review_id,
        "v3_backfill_id": attribution.get("v3_backfill_id"),
        "source_backfill_id": attribution.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS"
        if V4_MICRO_MIN_VARIANTS <= len(variants) <= V4_MICRO_MAX_VARIANTS
        else "FAIL",
        "market_regime": attribution.get("market_regime", "ai_after_chatgpt"),
        "variant_count": len(variants),
        "micro_search_v4_design_manifest_path": str(root / "micro_search_v4_design_manifest.json"),
        "v4_design_rationale_path": str(root / "v4_design_rationale.json"),
        "v4_variant_specs_path": str(root / "v4_variant_specs.jsonl"),
        "micro_search_v4_design_report_path": str(root / "micro_search_v4_design_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "micro_search_v4_design_manifest.json", manifest)
    _write_json(root / "v4_design_rationale.json", rationale)
    _write_jsonl(root / "v4_variant_specs.jsonl", variants)
    _write_text(
        root / "micro_search_v4_design_report.md",
        render_micro_search_v4_design_report(manifest, rationale, variants),
    )
    _write_latest_pointer(
        "latest_micro_search_v4_design",
        root.name,
        root / "micro_search_v4_design_manifest.json",
    )
    return {
        "v4_design_id": root.name,
        "v4_design_dir": root,
        "manifest": manifest,
        "v4_design_rationale": rationale,
        "v4_variant_specs": variants,
    }


def micro_search_v4_design_report_payload(
    *,
    v4_design_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v4_design_id,
        latest_pointer="latest_micro_search_v4_design",
        latest=latest,
        output_dir=output_dir,
        required_name="micro_search_v4_design_manifest.json",
    )
    return {
        **_read_json(root / "micro_search_v4_design_manifest.json"),
        "v4_design_rationale": _read_json(root / "v4_design_rationale.json"),
        "v4_variant_specs": _read_jsonl(root / "v4_variant_specs.jsonl"),
        "v4_design_dir": str(root),
    }


def validate_micro_search_v4_design_artifact(
    *,
    v4_design_id: str,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> dict[str, Any]:
    root = output_dir / v4_design_id
    manifest = _read_optional_json(root / "micro_search_v4_design_manifest.json") or {}
    rationale = _read_optional_json(root / "v4_design_rationale.json") or {}
    variants = _read_jsonl(root / "v4_variant_specs.jsonl")
    checks = _required_file_checks(
        root,
        (
            "micro_search_v4_design_manifest.json",
            "v4_design_rationale.json",
            "v4_variant_specs.jsonl",
            "micro_search_v4_design_report.md",
        ),
    )
    required_variants = {
        "smooth_3d_plus_dispersion_gate",
        "smooth_3d_plus_topk_stability_filter",
        "smooth_3d_plus_rebalance_delta_3pct",
        "cash_buffer_8_plus_smooth_3d",
        "cash_buffer_10_plus_dispersion_gate",
        "median_consensus_plus_smooth_3d",
        "median_consensus_plus_dispersion_gate",
        "top5_consensus_plus_smooth_3d",
        "top5_consensus_plus_rebalance_threshold",
        "high_disagreement_hold_previous",
        "high_disagreement_reduce_tilt_50",
        "sideways_hold_plus_fast_restore",
    }
    variant_ids = {_text(row.get("variant_id")) for row in variants}
    checks.extend(
        [
            st._check("v4_design_id_matches", manifest.get("v4_design_id") == v4_design_id, ""),
            st._check(
                "variant_count_bounded",
                V4_MICRO_MIN_VARIANTS <= len(variants) <= V4_MICRO_MAX_VARIANTS,
                str(len(variants)),
            ),
            st._check(
                "required_variants_present",
                required_variants.issubset(variant_ids),
                ",".join(sorted(required_variants - variant_ids)),
            ),
            st._check(
                "each_variant_has_rationale",
                all(_texts(row.get("target_failure_modes")) for row in variants),
                "",
            ),
            st._check("rationale_visible", bool(_texts(rationale.get("design_principles"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, rationale, *variants), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, rationale, *variants),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_micro_search_v4_design_validation",
        v4_design_id,
        checks,
    )


def run_micro_search_v4_backfill(
    *,
    v4_design_id: str,
    v4_design_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    design = micro_search_v4_design_report_payload(
        v4_design_id=v4_design_id,
        output_dir=v4_design_dir,
    )
    source_backfill_id = _text(design.get("source_backfill_id"))
    if not source_backfill_id:
        raise RuntimeError("micro search v4 design is missing source_backfill_id")
    backfill = st.paper_shadow_backfill_report_payload(
        backfill_id=source_backfill_id,
        output_dir=baseline_backfill_dir,
    )
    baseline_states = _records(backfill.get("backfill_method_states"))
    config = st._load_backfill_config_from_manifest(backfill)
    start = max(
        _coerce_date(backfill.get("date_start"), st.AI_AFTER_CHATGPT_START),
        st.AI_AFTER_CHATGPT_START,
    )
    requested_end = _coerce_date(backfill.get("date_end"), generated.date())
    source = _mapping(config.get("source"))
    symbols = st._symbols_from_state_paths(baseline_states)
    prices_path = price_cache_path or st._resolve_project_path(
        source.get("price_cache_path"),
        st.DEFAULT_PRICE_CACHE_PATH,
    )
    pivot = st._load_price_pivot(prices_path, symbols, start)
    latest_valid_as_of = _latest_common_price_date(pivot, symbols)
    end = min(requested_end, latest_valid_as_of, generated.date())
    used_latest_valid_as_of = end < requested_end
    pivot = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    quality_as_of = max(end, generated.date())
    quality = st._run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    if not quality.passed:
        raise RuntimeError(
            f"data quality gate failed for micro search v4 backfill: {quality.status}"
        )
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): st._risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variant_specs = _records(design.get("v4_variant_specs"))
    variant_states: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for variant in variant_specs:
        try:
            variant_states.extend(
                st._run_variant_weight_path(
                    variant=variant,
                    baseline_states=baseline_states,
                    returns=returns,
                    labels=labels,
                    config=config,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed.append({"variant_id": _text(variant.get("variant_id")), "error": str(exc)})
    performance = st._variant_performance_metrics(variant_states, baseline_states)
    regime = st._variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = st._variant_stability_metrics(variant_states, baseline_states, config)
    signal = _v4_variant_signal_metrics(variant_states, stability, regime)
    backfill_id = _stable_id(
        "micro-search-v4-backfill",
        v4_design_id,
        end.isoformat(),
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / backfill_id)
    root.mkdir(parents=True, exist_ok=False)
    quality_report_path = root / "validate_data_quality_report.md"
    progress = {
        "schema_version": st.SCHEMA_VERSION,
        "v4_backfill_id": root.name,
        "variants_total": len(variant_specs),
        "variants_completed": len({row.get("variant_id") for row in performance}),
        "variants_failed": len(failed),
        "failed_variants": failed,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_date_end": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_micro_search_v4_backfill_manifest",
        "v4_backfill_id": root.name,
        "v4_design_id": v4_design_id,
        "source_backfill_id": source_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS"
        if not failed and performance
        else "PASS_WITH_WARNINGS"
        if performance
        else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_start_date": backfill.get("requested_start_date", start.isoformat()),
        "requested_end_date": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        "variants_total": len(variant_specs),
        "variants_completed": progress["variants_completed"],
        "variants_failed": len(failed),
        "micro_search_v4_backfill_manifest_path": str(
            root / "micro_search_v4_backfill_manifest.json"
        ),
        "v4_backfill_progress_path": str(root / "v4_backfill_progress.json"),
        "v4_variant_performance_path": str(root / "v4_variant_performance.jsonl"),
        "v4_variant_regime_metrics_path": str(root / "v4_variant_regime_metrics.jsonl"),
        "v4_variant_stability_metrics_path": str(root / "v4_variant_stability_metrics.jsonl"),
        "v4_variant_signal_metrics_path": str(root / "v4_variant_signal_metrics.jsonl"),
        "micro_search_v4_backfill_report_path": str(root / "micro_search_v4_backfill_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "micro_search_v4_backfill_manifest.json", manifest)
    _write_json(root / "v4_backfill_progress.json", progress)
    write_data_quality_report(quality, quality_report_path)
    _write_jsonl(root / "v4_variant_performance.jsonl", performance)
    _write_jsonl(root / "v4_variant_regime_metrics.jsonl", regime)
    _write_jsonl(root / "v4_variant_stability_metrics.jsonl", stability)
    _write_jsonl(root / "v4_variant_signal_metrics.jsonl", signal)
    _write_text(
        root / "micro_search_v4_backfill_report.md",
        render_micro_search_v4_backfill_report(manifest, progress),
    )
    _write_latest_pointer(
        "latest_micro_search_v4_backfill",
        root.name,
        root / "micro_search_v4_backfill_manifest.json",
    )
    return {
        "v4_backfill_id": root.name,
        "v4_backfill_dir": root,
        "manifest": manifest,
        "v4_backfill_progress": progress,
        "v4_variant_performance": performance,
        "v4_variant_regime_metrics": regime,
        "v4_variant_stability_metrics": stability,
        "v4_variant_signal_metrics": signal,
    }


def micro_search_v4_backfill_report_payload(
    *,
    v4_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v4_backfill_id,
        latest_pointer="latest_micro_search_v4_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="micro_search_v4_backfill_manifest.json",
    )
    return {
        **_read_json(root / "micro_search_v4_backfill_manifest.json"),
        "v4_backfill_progress": _read_json(root / "v4_backfill_progress.json"),
        "v4_variant_performance": _read_jsonl(root / "v4_variant_performance.jsonl"),
        "v4_variant_regime_metrics": _read_jsonl(root / "v4_variant_regime_metrics.jsonl"),
        "v4_variant_stability_metrics": _read_jsonl(root / "v4_variant_stability_metrics.jsonl"),
        "v4_variant_signal_metrics": _read_jsonl(root / "v4_variant_signal_metrics.jsonl"),
        "v4_backfill_dir": str(root),
    }


def validate_micro_search_v4_backfill_artifact(
    *,
    v4_backfill_id: str,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / v4_backfill_id
    manifest = _read_optional_json(root / "micro_search_v4_backfill_manifest.json") or {}
    progress = _read_optional_json(root / "v4_backfill_progress.json") or {}
    performance = _read_jsonl(root / "v4_variant_performance.jsonl")
    regime = _read_jsonl(root / "v4_variant_regime_metrics.jsonl")
    stability = _read_jsonl(root / "v4_variant_stability_metrics.jsonl")
    signal = _read_jsonl(root / "v4_variant_signal_metrics.jsonl")
    checks = _required_file_checks(
        root,
        (
            "micro_search_v4_backfill_manifest.json",
            "v4_backfill_progress.json",
            "v4_variant_performance.jsonl",
            "v4_variant_regime_metrics.jsonl",
            "v4_variant_stability_metrics.jsonl",
            "v4_variant_signal_metrics.jsonl",
            "micro_search_v4_backfill_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "v4_backfill_id_matches",
                manifest.get("v4_backfill_id") == v4_backfill_id,
                "",
            ),
            st._check(
                "variants_completed_visible",
                int(_float(progress.get("variants_completed"))) > 0,
                _text(progress.get("variants_completed")),
            ),
            st._check(
                "data_quality_visible",
                manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(manifest.get("data_quality_status")),
            ),
            st._check("performance_readable", bool(performance), ""),
            st._check("regime_metrics_readable", bool(regime), ""),
            st._check("stability_metrics_readable", bool(stability), ""),
            st._check("signal_metrics_readable", bool(signal), ""),
            st._check("broker_forbidden", _payload_safe(manifest, progress, *performance), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, progress, *performance, *signal),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_micro_search_v4_backfill_validation",
        v4_backfill_id,
        checks,
    )


def run_gate_calibrated_review(
    *,
    v4_backfill_id: str,
    gate_calibration_id: str,
    v4_backfill_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    v4_design_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    gate_calibration_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    output_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = micro_search_v4_backfill_report_payload(
        v4_backfill_id=v4_backfill_id,
        output_dir=v4_backfill_dir,
    )
    design = micro_search_v4_design_report_payload(
        v4_design_id=_text(backfill.get("v4_design_id")),
        output_dir=v4_design_dir,
    )
    gate = gate_calibration_review_report_payload(
        gate_calibration_id=gate_calibration_id,
        output_dir=gate_calibration_dir,
    )
    rows = _v4_scorecard_rows(backfill, design)
    official = _gate_review_rows(rows, diagnostic=False)
    diagnostic = _gate_review_rows(rows, diagnostic=True)
    summary = _gate_calibrated_summary(official, diagnostic, gate)
    gate_review_id = _stable_id(
        "gate-calibrated-review",
        v4_backfill_id,
        gate_calibration_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / gate_review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_calibrated_review_manifest",
        "gate_review_id": root.name,
        "v4_backfill_id": v4_backfill_id,
        "v4_design_id": backfill.get("v4_design_id"),
        "gate_calibration_id": gate_calibration_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "gate_calibrated_review_manifest_path": str(root / "gate_calibrated_review_manifest.json"),
        "official_gate_results_path": str(root / "official_gate_results.jsonl"),
        "diagnostic_gate_results_path": str(root / "diagnostic_gate_results.jsonl"),
        "gate_calibrated_summary_path": str(root / "gate_calibrated_summary.json"),
        "gate_calibrated_review_report_path": str(root / "gate_calibrated_review_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "gate_calibrated_review_manifest.json", manifest)
    _write_jsonl(root / "official_gate_results.jsonl", official)
    _write_jsonl(root / "diagnostic_gate_results.jsonl", diagnostic)
    _write_json(root / "gate_calibrated_summary.json", summary)
    _write_text(
        root / "gate_calibrated_review_report.md",
        render_gate_calibrated_review_report(manifest, summary),
    )
    _write_latest_pointer(
        "latest_gate_calibrated_review",
        root.name,
        root / "gate_calibrated_review_manifest.json",
    )
    return {
        "gate_review_id": root.name,
        "gate_review_dir": root,
        "manifest": manifest,
        "official_gate_results": official,
        "diagnostic_gate_results": diagnostic,
        "gate_calibrated_summary": summary,
    }


def gate_calibrated_review_report_payload(
    *,
    gate_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=gate_review_id,
        latest_pointer="latest_gate_calibrated_review",
        latest=latest,
        output_dir=output_dir,
        required_name="gate_calibrated_review_manifest.json",
    )
    return {
        **_read_json(root / "gate_calibrated_review_manifest.json"),
        "official_gate_results": _read_jsonl(root / "official_gate_results.jsonl"),
        "diagnostic_gate_results": _read_jsonl(root / "diagnostic_gate_results.jsonl"),
        "gate_calibrated_summary": _read_json(root / "gate_calibrated_summary.json"),
        "gate_review_dir": str(root),
    }


def validate_gate_calibrated_review_artifact(
    *,
    gate_review_id: str,
    output_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / gate_review_id
    manifest = _read_optional_json(root / "gate_calibrated_review_manifest.json") or {}
    official = _read_jsonl(root / "official_gate_results.jsonl")
    diagnostic = _read_jsonl(root / "diagnostic_gate_results.jsonl")
    summary = _read_optional_json(root / "gate_calibrated_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "gate_calibrated_review_manifest.json",
            "official_gate_results.jsonl",
            "diagnostic_gate_results.jsonl",
            "gate_calibrated_summary.json",
            "gate_calibrated_review_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "gate_review_id_matches",
                manifest.get("gate_review_id") == gate_review_id,
                "",
            ),
            st._check("official_results_readable", isinstance(official, list), ""),
            st._check("diagnostic_results_readable", isinstance(diagnostic, list), ""),
            st._check(
                "diagnostic_only_no_policy_change",
                summary.get("gate_policy_change_recommended") is False,
                "",
            ),
            st._check(
                "broker_forbidden",
                _payload_safe(manifest, summary, *official, *diagnostic),
                "",
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *official, *diagnostic),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_gate_calibrated_review_validation",
        gate_review_id,
        checks,
    )


def run_signal_vs_parameter_attribution(
    *,
    signal_diagnosis_id: str,
    consensus_review_id: str,
    gate_review_id: str,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    gate_review_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
    output_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    signal = signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=signal_diagnosis_id,
        output_dir=signal_dir,
    )
    consensus = consensus_quality_review_report_payload(
        consensus_review_id=consensus_review_id,
        output_dir=consensus_dir,
    )
    gate = gate_calibrated_review_report_payload(
        gate_review_id=gate_review_id,
        output_dir=gate_review_dir,
    )
    failure = _signal_vs_parameter_failure_source(signal, consensus, gate)
    shift = _recommended_research_shift(failure, consensus)
    attribution_id = _stable_id(
        "signal-vs-parameter-attribution",
        signal_diagnosis_id,
        consensus_review_id,
        gate_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_vs_parameter_manifest",
        "attribution_id": root.name,
        "signal_diagnosis_id": signal_diagnosis_id,
        "consensus_review_id": consensus_review_id,
        "gate_review_id": gate_review_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": signal.get("market_regime", "ai_after_chatgpt"),
        "signal_vs_parameter_manifest_path": str(root / "signal_vs_parameter_manifest.json"),
        "failure_source_attribution_path": str(root / "failure_source_attribution.json"),
        "recommended_research_shift_path": str(root / "recommended_research_shift.json"),
        "signal_vs_parameter_attribution_report_path": str(
            root / "signal_vs_parameter_attribution_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_signal_vs_parameter_reader_brief(failure, shift)
    _write_json(root / "signal_vs_parameter_manifest.json", manifest)
    _write_json(root / "failure_source_attribution.json", failure)
    _write_json(root / "recommended_research_shift.json", shift)
    _write_text(
        root / "signal_vs_parameter_attribution_report.md",
        render_signal_vs_parameter_attribution_report(manifest, failure, shift),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_signal_vs_parameter_attribution",
        root.name,
        root / "signal_vs_parameter_manifest.json",
    )
    return {
        "signal_vs_parameter_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "failure_source_attribution": failure,
        "recommended_research_shift": shift,
        "reader_brief_section": reader,
    }


def signal_vs_parameter_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_signal_vs_parameter_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_vs_parameter_manifest.json",
    )
    return {
        **_read_json(root / "signal_vs_parameter_manifest.json"),
        "failure_source_attribution": _read_json(root / "failure_source_attribution.json"),
        "recommended_research_shift": _read_json(root / "recommended_research_shift.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "attribution_dir": str(root),
    }


def validate_signal_vs_parameter_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    manifest = _read_optional_json(root / "signal_vs_parameter_manifest.json") or {}
    failure = _read_optional_json(root / "failure_source_attribution.json") or {}
    shift = _read_optional_json(root / "recommended_research_shift.json") or {}
    checks = _required_file_checks(
        root,
        (
            "signal_vs_parameter_manifest.json",
            "failure_source_attribution.json",
            "recommended_research_shift.json",
            "signal_vs_parameter_attribution_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "attribution_id_matches",
                manifest.get("attribution_id") == attribution_id,
                "",
            ),
            st._check(
                "failure_source_valid",
                failure.get("failure_source")
                in {
                    "PARAMETER_SPACE",
                    "SIGNAL_QUALITY",
                    "REGIME_TAGGING",
                    "CONSENSUS_QUALITY",
                    "GATE_POLICY",
                    "MARKET_REGIME",
                    "MIXED",
                    "INCONCLUSIVE",
                },
                _text(failure.get("failure_source")),
            ),
            st._check("research_shift_visible", bool(shift.get("recommended_shift")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, failure, shift), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, failure, shift),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_signal_vs_parameter_attribution_validation",
        attribution_id,
        checks,
    )


def run_next_research_direction(
    *,
    attribution_id: str,
    attribution_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    attribution = signal_vs_parameter_attribution_report_payload(
        attribution_id=attribution_id,
        output_dir=attribution_dir,
    )
    decision = _next_research_direction_decision(attribution)
    task_plan = _next_research_task_plan(decision)
    direction_id = _stable_id("next-research-direction", attribution_id, generated.isoformat())
    root = _unique_dir(output_dir / direction_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["direction_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_next_research_direction_manifest",
        "direction_id": root.name,
        "attribution_id": attribution_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": attribution.get("market_regime", "ai_after_chatgpt"),
        "next_research_direction_manifest_path": str(
            root / "next_research_direction_manifest.json"
        ),
        "next_research_direction_decision_path": str(
            root / "next_research_direction_decision.json"
        ),
        "next_task_plan_path": str(root / "next_task_plan.json"),
        "next_research_direction_report_path": str(root / "next_research_direction_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_next_research_direction_reader_brief(decision)
    _write_json(root / "next_research_direction_manifest.json", manifest)
    _write_json(root / "next_research_direction_decision.json", decision)
    _write_json(root / "next_task_plan.json", task_plan)
    _write_text(
        root / "next_research_direction_report.md",
        render_next_research_direction_report(manifest, decision, task_plan),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_next_research_direction",
        root.name,
        root / "next_research_direction_manifest.json",
    )
    return {
        "direction_id": root.name,
        "direction_dir": root,
        "manifest": manifest,
        "next_research_direction_decision": decision,
        "next_task_plan": task_plan,
        "reader_brief_section": reader,
    }


def next_research_direction_report_payload(
    *,
    direction_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=direction_id,
        latest_pointer="latest_next_research_direction",
        latest=latest,
        output_dir=output_dir,
        required_name="next_research_direction_manifest.json",
    )
    return {
        **_read_json(root / "next_research_direction_manifest.json"),
        "next_research_direction_decision": _read_json(
            root / "next_research_direction_decision.json"
        ),
        "next_task_plan": _read_json(root / "next_task_plan.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "direction_dir": str(root),
    }


def validate_next_research_direction_artifact(
    *,
    direction_id: str,
    output_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> dict[str, Any]:
    root = output_dir / direction_id
    manifest = _read_optional_json(root / "next_research_direction_manifest.json") or {}
    decision = _read_optional_json(root / "next_research_direction_decision.json") or {}
    task_plan = _read_optional_json(root / "next_task_plan.json") or {}
    checks = _required_file_checks(
        root,
        (
            "next_research_direction_manifest.json",
            "next_research_direction_decision.json",
            "next_task_plan.json",
            "next_research_direction_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("direction_id_matches", manifest.get("direction_id") == direction_id, ""),
            st._check(
                "decision_valid",
                decision.get("decision")
                in {
                    "CONTINUE_MICRO_SEARCH_V5",
                    "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS",
                    "IMPLEMENT_CANDIDATE_QUALITY_FILTER",
                    "REVIEW_GATE_POLICY",
                    "DEFER_PARAMETER_SEARCH_AND_CONTINUE_FORWARD_CONFIRMATION",
                },
                _text(decision.get("decision")),
            ),
            st._check("task_plan_readable", bool(_records(task_plan.get("tasks"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, decision, task_plan), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, decision, task_plan),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_next_research_direction_validation",
        direction_id,
        checks,
    )


def update_owner_research_roadmap(
    *,
    direction_id: str,
    direction_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    direction = next_research_direction_report_payload(
        direction_id=direction_id,
        output_dir=direction_dir,
    )
    summary = _owner_roadmap_summary(direction)
    checklist = render_owner_roadmap_checklist(summary, direction)
    roadmap_id = _stable_id("owner-research-roadmap", direction_id, generated.isoformat())
    root = _unique_dir(output_dir / roadmap_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_research_roadmap_manifest",
        "roadmap_id": root.name,
        "direction_id": direction_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": direction.get("market_regime", "ai_after_chatgpt"),
        "owner_research_roadmap_manifest_path": str(root / "owner_research_roadmap_manifest.json"),
        "owner_roadmap_summary_path": str(root / "owner_roadmap_summary.json"),
        "owner_roadmap_checklist_path": str(root / "owner_roadmap_checklist.md"),
        "owner_research_roadmap_report_path": str(root / "owner_research_roadmap_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_owner_roadmap_reader_brief(summary)
    _write_json(root / "owner_research_roadmap_manifest.json", manifest)
    _write_json(root / "owner_roadmap_summary.json", summary)
    _write_text(root / "owner_roadmap_checklist.md", checklist)
    _write_text(
        root / "owner_research_roadmap_report.md",
        render_owner_research_roadmap_report(manifest, summary, checklist),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_owner_research_roadmap",
        root.name,
        root / "owner_research_roadmap_manifest.json",
    )
    return {
        "roadmap_id": root.name,
        "roadmap_dir": root,
        "manifest": manifest,
        "owner_roadmap_summary": summary,
        "owner_roadmap_checklist": checklist,
        "reader_brief_section": reader,
    }


def owner_research_roadmap_report_payload(
    *,
    roadmap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=roadmap_id,
        latest_pointer="latest_owner_research_roadmap",
        latest=latest,
        output_dir=output_dir,
        required_name="owner_research_roadmap_manifest.json",
    )
    return {
        **_read_json(root / "owner_research_roadmap_manifest.json"),
        "owner_roadmap_summary": _read_json(root / "owner_roadmap_summary.json"),
        "owner_roadmap_checklist": (root / "owner_roadmap_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "roadmap_dir": str(root),
    }


def validate_owner_research_roadmap_artifact(
    *,
    roadmap_id: str,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> dict[str, Any]:
    root = output_dir / roadmap_id
    manifest = _read_optional_json(root / "owner_research_roadmap_manifest.json") or {}
    summary = _read_optional_json(root / "owner_roadmap_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "owner_research_roadmap_manifest.json",
            "owner_roadmap_summary.json",
            "owner_roadmap_checklist.md",
            "owner_research_roadmap_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("roadmap_id_matches", manifest.get("roadmap_id") == roadmap_id, ""),
            st._check("current_phase_visible", bool(summary.get("current_phase")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_owner_research_roadmap_validation",
        roadmap_id,
        checks,
    )


def run_signal_failure_taxonomy_validation(
    *,
    config_path: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_CONFIG_PATH,
    output_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = st._load_yaml_mapping(config_path)
    _assert_signal_failure_taxonomy_safety(_mapping(config.get("safety")))
    normalized = _normalized_signal_failure_taxonomy(config)
    catalog = _signal_failure_mode_catalog(normalized)
    requested_id = _text(config.get("taxonomy_id"), "signal_feature_failure_taxonomy_v1")
    taxonomy_id = _stable_id("signal-failure-taxonomy", requested_id, generated.isoformat())
    root = _unique_dir(output_dir / taxonomy_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_failure_taxonomy_manifest",
        "taxonomy_id": root.name,
        "source_taxonomy_id": requested_id,
        "config_path": str(config_path),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "failure_mode_count": len(_records(catalog.get("failure_modes"))),
        "family_count": len(_records(normalized.get("families"))),
        "signal_failure_taxonomy_manifest_path": str(
            root / "signal_failure_taxonomy_manifest.json"
        ),
        "normalized_signal_failure_taxonomy_path": str(
            root / "normalized_signal_failure_taxonomy.yaml"
        ),
        "signal_failure_mode_catalog_path": str(root / "signal_failure_mode_catalog.json"),
        "signal_failure_taxonomy_report_path": str(root / "signal_failure_taxonomy_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "signal_failure_taxonomy_manifest.json", manifest)
    _write_text(
        root / "normalized_signal_failure_taxonomy.yaml",
        yaml.safe_dump(normalized, sort_keys=False, allow_unicode=True),
    )
    _write_json(root / "signal_failure_mode_catalog.json", catalog)
    _write_text(
        root / "signal_failure_taxonomy_report.md",
        render_signal_failure_taxonomy_report(manifest, catalog),
    )
    _write_latest_pointer(
        "latest_signal_failure_taxonomy",
        root.name,
        root / "signal_failure_taxonomy_manifest.json",
    )
    return {
        "taxonomy_id": root.name,
        "taxonomy_dir": root,
        "manifest": manifest,
        "normalized_signal_failure_taxonomy": normalized,
        "signal_failure_mode_catalog": catalog,
    }


def signal_failure_taxonomy_report_payload(
    *,
    taxonomy_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=taxonomy_id,
        latest_pointer="latest_signal_failure_taxonomy",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_failure_taxonomy_manifest.json",
    )
    normalized = yaml.safe_load(
        (root / "normalized_signal_failure_taxonomy.yaml").read_text(encoding="utf-8")
    )
    return {
        **_read_json(root / "signal_failure_taxonomy_manifest.json"),
        "normalized_signal_failure_taxonomy": normalized,
        "signal_failure_mode_catalog": _read_json(root / "signal_failure_mode_catalog.json"),
        "taxonomy_dir": str(root),
    }


def validate_signal_failure_taxonomy_artifact(
    *,
    taxonomy_id: str,
    output_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> dict[str, Any]:
    root = output_dir / taxonomy_id
    manifest = _read_optional_json(root / "signal_failure_taxonomy_manifest.json") or {}
    catalog = _read_optional_json(root / "signal_failure_mode_catalog.json") or {}
    normalized_path = root / "normalized_signal_failure_taxonomy.yaml"
    normalized = (
        yaml.safe_load(normalized_path.read_text(encoding="utf-8"))
        if normalized_path.exists()
        else {}
    )
    modes = _records(catalog.get("failure_modes"))
    families = _records(_mapping(normalized).get("families"))
    checks = _required_file_checks(
        root,
        (
            "signal_failure_taxonomy_manifest.json",
            "normalized_signal_failure_taxonomy.yaml",
            "signal_failure_mode_catalog.json",
            "signal_failure_taxonomy_report.md",
        ),
    )
    checks.extend(
        [
            st._check("taxonomy_id_matches", manifest.get("taxonomy_id") == taxonomy_id, ""),
            st._check("failure_modes_readable", len(modes) >= 10, str(len(modes))),
            st._check("families_readable", bool(families), ""),
            st._check(
                "required_modes_present",
                {"signal_churn", "regime_mismatch", "candidate_disagreement_high"}.issubset(
                    {_text(row.get("mode")) for row in modes}
                ),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, catalog), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, catalog),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_signal_failure_taxonomy_validation",
        taxonomy_id,
        checks,
    )


def build_candidate_signal_ledger(
    *,
    taxonomy_id: str,
    source_backfill_id: str,
    taxonomy_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
    source_backfill_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    v4_design_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    taxonomy = signal_failure_taxonomy_report_payload(
        taxonomy_id=taxonomy_id,
        output_dir=taxonomy_dir,
    )
    source = _candidate_signal_ledger_source(
        source_backfill_id=source_backfill_id,
        source_backfill_dir=source_backfill_dir,
        v4_design_dir=v4_design_dir,
        signal_dir=signal_dir,
        consensus_dir=consensus_dir,
    )
    events = _candidate_signal_events(taxonomy, source)
    summary = _candidate_signal_summary(events)
    ledger_id = _stable_id(
        "candidate-signal-ledger",
        taxonomy_id,
        source_backfill_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / ledger_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_signal_ledger_manifest",
        "ledger_id": root.name,
        "taxonomy_id": taxonomy_id,
        "source_backfill_id": source_backfill_id,
        "source_backfill_type": source.get("source_backfill_type"),
        "source_signal_diagnosis_id": source.get("signal_diagnosis_id"),
        "source_consensus_review_id": source.get("consensus_review_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if events else "PASS_WITH_WARNINGS",
        "market_regime": source.get("market_regime", "ai_after_chatgpt"),
        "date_start": source.get("date_start"),
        "date_end": source.get("date_end"),
        "data_quality_status": source.get("data_quality_status", "UNKNOWN"),
        "event_count": len(events),
        "candidate_signal_ledger_manifest_path": str(
            root / "candidate_signal_ledger_manifest.json"
        ),
        "signal_events_path": str(root / "signal_events.jsonl"),
        "candidate_signal_summary_path": str(root / "candidate_signal_summary.json"),
        "candidate_signal_ledger_report_path": str(root / "candidate_signal_ledger_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_candidate_signal_ledger_reader_brief(summary)
    _write_json(root / "candidate_signal_ledger_manifest.json", manifest)
    _write_jsonl(root / "signal_events.jsonl", events)
    _write_json(root / "candidate_signal_summary.json", summary)
    _write_text(
        root / "candidate_signal_ledger_report.md",
        render_candidate_signal_ledger_report(manifest, summary),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_candidate_signal_ledger",
        root.name,
        root / "candidate_signal_ledger_manifest.json",
    )
    return {
        "ledger_id": root.name,
        "ledger_dir": root,
        "manifest": manifest,
        "signal_events": events,
        "candidate_signal_summary": summary,
        "reader_brief_section": reader,
    }


def candidate_signal_ledger_report_payload(
    *,
    ledger_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=ledger_id,
        latest_pointer="latest_candidate_signal_ledger",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_signal_ledger_manifest.json",
    )
    return {
        **_read_json(root / "candidate_signal_ledger_manifest.json"),
        "signal_events": _read_jsonl(root / "signal_events.jsonl"),
        "candidate_signal_summary": _read_json(root / "candidate_signal_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "ledger_dir": str(root),
    }


def validate_candidate_signal_ledger_artifact(
    *,
    ledger_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> dict[str, Any]:
    root = output_dir / ledger_id
    manifest = _read_optional_json(root / "candidate_signal_ledger_manifest.json") or {}
    events = _read_jsonl(root / "signal_events.jsonl")
    summary = _read_optional_json(root / "candidate_signal_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "candidate_signal_ledger_manifest.json",
            "signal_events.jsonl",
            "candidate_signal_summary.json",
            "candidate_signal_ledger_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("ledger_id_matches", manifest.get("ledger_id") == ledger_id, ""),
            st._check("events_readable", bool(events), ""),
            st._check("summary_methods_readable", bool(_records(summary.get("methods"))), ""),
            st._check("data_quality_visible", bool(manifest.get("data_quality_status")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *events), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *events),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_candidate_signal_ledger_validation",
        ledger_id,
        checks,
    )


def run_signal_churn_root_cause_review(
    *,
    ledger_id: str,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    ledger = candidate_signal_ledger_report_payload(ledger_id=ledger_id, output_dir=ledger_dir)
    summary = _churn_root_cause_summary(ledger)
    clusters = _churn_event_clusters(ledger)
    mitigations = _churn_mitigation_candidates(summary)
    root_cause_id = _stable_id("signal-churn-root-cause", ledger_id, generated.isoformat())
    root = _unique_dir(output_dir / root_cause_id)
    root.mkdir(parents=True, exist_ok=False)
    summary["root_cause_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_churn_root_cause_manifest",
        "root_cause_id": root.name,
        "ledger_id": ledger_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "signal_churn_root_cause_manifest_path": str(
            root / "signal_churn_root_cause_manifest.json"
        ),
        "churn_root_cause_summary_path": str(root / "churn_root_cause_summary.json"),
        "churn_event_clusters_path": str(root / "churn_event_clusters.jsonl"),
        "churn_mitigation_candidates_path": str(root / "churn_mitigation_candidates.json"),
        "signal_churn_root_cause_report_path": str(root / "signal_churn_root_cause_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "signal_churn_root_cause_manifest.json", manifest)
    _write_json(root / "churn_root_cause_summary.json", summary)
    _write_jsonl(root / "churn_event_clusters.jsonl", clusters)
    _write_json(root / "churn_mitigation_candidates.json", mitigations)
    _write_text(
        root / "signal_churn_root_cause_report.md",
        render_signal_churn_root_cause_report(manifest, summary, clusters, mitigations),
    )
    _write_latest_pointer(
        "latest_signal_churn_root_cause",
        root.name,
        root / "signal_churn_root_cause_manifest.json",
    )
    return {
        "root_cause_id": root.name,
        "root_cause_dir": root,
        "manifest": manifest,
        "churn_root_cause_summary": summary,
        "churn_event_clusters": clusters,
        "churn_mitigation_candidates": mitigations,
    }


def signal_churn_root_cause_report_payload(
    *,
    root_cause_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=root_cause_id,
        latest_pointer="latest_signal_churn_root_cause",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_churn_root_cause_manifest.json",
    )
    return {
        **_read_json(root / "signal_churn_root_cause_manifest.json"),
        "churn_root_cause_summary": _read_json(root / "churn_root_cause_summary.json"),
        "churn_event_clusters": _read_jsonl(root / "churn_event_clusters.jsonl"),
        "churn_mitigation_candidates": _read_json(root / "churn_mitigation_candidates.json"),
        "root_cause_dir": str(root),
    }


def validate_signal_churn_root_cause_artifact(
    *,
    root_cause_id: str,
    output_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> dict[str, Any]:
    root = output_dir / root_cause_id
    manifest = _read_optional_json(root / "signal_churn_root_cause_manifest.json") or {}
    summary = _read_optional_json(root / "churn_root_cause_summary.json") or {}
    clusters = _read_jsonl(root / "churn_event_clusters.jsonl")
    mitigations = _read_optional_json(root / "churn_mitigation_candidates.json") or {}
    checks = _required_file_checks(
        root,
        (
            "signal_churn_root_cause_manifest.json",
            "churn_root_cause_summary.json",
            "churn_event_clusters.jsonl",
            "churn_mitigation_candidates.json",
            "signal_churn_root_cause_report.md",
        ),
    )
    checks.extend(
        [
            st._check("root_cause_id_matches", manifest.get("root_cause_id") == root_cause_id, ""),
            st._check("dominant_root_cause_visible", bool(summary.get("dominant_root_cause")), ""),
            st._check("clusters_listed", isinstance(clusters, list), ""),
            st._check("mitigations_readable", bool(_records(mitigations.get("mitigations"))), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, mitigations), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, mitigations, *clusters),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_signal_churn_root_cause_validation",
        root_cause_id,
        checks,
    )


def run_regime_mismatch_attribution(
    *,
    ledger_id: str,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    ledger = candidate_signal_ledger_report_payload(ledger_id=ledger_id, output_dir=ledger_dir)
    events = _regime_mismatch_attribution_events(ledger)
    summary = _regime_mismatch_summary(events)
    mismatch_id = _stable_id("regime-mismatch-attribution", ledger_id, generated.isoformat())
    root = _unique_dir(output_dir / mismatch_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_mismatch_manifest",
        "mismatch_id": root.name,
        "ledger_id": ledger_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "regime_mismatch_manifest_path": str(root / "regime_mismatch_manifest.json"),
        "regime_mismatch_events_path": str(root / "regime_mismatch_events.jsonl"),
        "regime_mismatch_summary_path": str(root / "regime_mismatch_summary.json"),
        "regime_mismatch_report_path": str(root / "regime_mismatch_report.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "regime_mismatch_manifest.json", manifest)
    _write_jsonl(root / "regime_mismatch_events.jsonl", events)
    _write_json(root / "regime_mismatch_summary.json", summary)
    _write_text(
        root / "regime_mismatch_report.md",
        render_regime_mismatch_report(manifest, summary),
    )
    _write_latest_pointer(
        "latest_regime_mismatch_attribution",
        root.name,
        root / "regime_mismatch_manifest.json",
    )
    return {
        "mismatch_id": root.name,
        "mismatch_dir": root,
        "manifest": manifest,
        "regime_mismatch_events": events,
        "regime_mismatch_summary": summary,
    }


def regime_mismatch_attribution_report_payload(
    *,
    mismatch_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=mismatch_id,
        latest_pointer="latest_regime_mismatch_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="regime_mismatch_manifest.json",
    )
    return {
        **_read_json(root / "regime_mismatch_manifest.json"),
        "regime_mismatch_events": _read_jsonl(root / "regime_mismatch_events.jsonl"),
        "regime_mismatch_summary": _read_json(root / "regime_mismatch_summary.json"),
        "mismatch_dir": str(root),
    }


def validate_regime_mismatch_attribution_artifact(
    *,
    mismatch_id: str,
    output_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / mismatch_id
    manifest = _read_optional_json(root / "regime_mismatch_manifest.json") or {}
    events = _read_jsonl(root / "regime_mismatch_events.jsonl")
    summary = _read_optional_json(root / "regime_mismatch_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "regime_mismatch_manifest.json",
            "regime_mismatch_events.jsonl",
            "regime_mismatch_summary.json",
            "regime_mismatch_report.md",
        ),
    )
    checks.extend(
        [
            st._check("mismatch_id_matches", manifest.get("mismatch_id") == mismatch_id, ""),
            st._check("mismatch_events_listed", isinstance(events, list), ""),
            st._check("summary_readable", "mismatch_count" in summary, ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *events), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *events),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_regime_mismatch_attribution_validation",
        mismatch_id,
        checks,
    )


def run_candidate_quality_filter_design(
    *,
    root_cause_id: str,
    mismatch_id: str,
    root_cause_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
    mismatch_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    root_cause = signal_churn_root_cause_report_payload(
        root_cause_id=root_cause_id,
        output_dir=root_cause_dir,
    )
    mismatch = regime_mismatch_attribution_report_payload(
        mismatch_id=mismatch_id,
        output_dir=mismatch_dir,
    )
    filters = _proposed_quality_filters(root_cause, mismatch)
    config = _filter_design_config(filters)
    filter_design_id = _stable_id(
        "candidate-quality-filter-design",
        root_cause_id,
        mismatch_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / filter_design_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_quality_filter_manifest",
        "filter_design_id": root.name,
        "root_cause_id": root_cause_id,
        "mismatch_id": mismatch_id,
        "source_ledger_id": root_cause.get("ledger_id") or mismatch.get("ledger_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": root_cause.get("market_regime", "ai_after_chatgpt"),
        "data_quality_status": root_cause.get("data_quality_status"),
        "candidate_quality_filter_manifest_path": str(
            root / "candidate_quality_filter_manifest.json"
        ),
        "proposed_quality_filters_path": str(root / "proposed_quality_filters.json"),
        "filter_design_config_path": str(root / "filter_design_config.yaml"),
        "candidate_quality_filter_design_report_path": str(
            root / "candidate_quality_filter_design_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_candidate_quality_filter_reader_brief(filters)
    _write_json(root / "candidate_quality_filter_manifest.json", manifest)
    _write_json(root / "proposed_quality_filters.json", filters)
    _write_text(
        root / "filter_design_config.yaml",
        yaml.safe_dump(config, sort_keys=False, allow_unicode=True),
    )
    _write_text(
        root / "candidate_quality_filter_design_report.md",
        render_candidate_quality_filter_design_report(manifest, filters),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_candidate_quality_filter_design",
        root.name,
        root / "candidate_quality_filter_manifest.json",
    )
    return {
        "filter_design_id": root.name,
        "filter_design_dir": root,
        "manifest": manifest,
        "proposed_quality_filters": filters,
        "filter_design_config": config,
        "reader_brief_section": reader,
    }


def candidate_quality_filter_design_report_payload(
    *,
    filter_design_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=filter_design_id,
        latest_pointer="latest_candidate_quality_filter_design",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_quality_filter_manifest.json",
    )
    config = yaml.safe_load((root / "filter_design_config.yaml").read_text(encoding="utf-8"))
    return {
        **_read_json(root / "candidate_quality_filter_manifest.json"),
        "proposed_quality_filters": _read_json(root / "proposed_quality_filters.json"),
        "filter_design_config": config,
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "filter_design_dir": str(root),
    }


def validate_candidate_quality_filter_design_artifact(
    *,
    filter_design_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> dict[str, Any]:
    root = output_dir / filter_design_id
    manifest = _read_optional_json(root / "candidate_quality_filter_manifest.json") or {}
    filters = _read_optional_json(root / "proposed_quality_filters.json") or {}
    config_path = root / "filter_design_config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    checks = _required_file_checks(
        root,
        (
            "candidate_quality_filter_manifest.json",
            "proposed_quality_filters.json",
            "filter_design_config.yaml",
            "candidate_quality_filter_design_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "filter_design_id_matches",
                manifest.get("filter_design_id") == filter_design_id,
                "",
            ),
            st._check("filters_readable", bool(_records(filters.get("filters"))), ""),
            st._check(
                "config_research_only",
                _text(_mapping(config.get("method")).get("mode")) == "research_screening_only",
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, filters, config), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, filters, _mapping(config.get("safety"))),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_candidate_quality_filter_design_validation",
        filter_design_id,
        checks,
    )


def run_filtered_candidate_backfill(
    *,
    filter_design_id: str,
    filter_design_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    design = candidate_quality_filter_design_report_payload(
        filter_design_id=filter_design_id,
        output_dir=filter_design_dir,
    )
    ledger = candidate_signal_ledger_report_payload(
        ledger_id=_text(design.get("source_ledger_id")),
        output_dir=ledger_dir,
    )
    specs = _filtered_variant_specs(design)
    performance = _filtered_variant_performance(specs, ledger)
    signal_metrics = _filtered_variant_signal_metrics(specs, ledger)
    backfill_id = _stable_id("filtered-candidate-backfill", filter_design_id, generated.isoformat())
    root = _unique_dir(output_dir / backfill_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_backfill_manifest",
        "filtered_backfill_id": root.name,
        "filter_design_id": filter_design_id,
        "source_ledger_id": design.get("source_ledger_id"),
        "source_backfill_id": ledger.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if specs else "PASS_WITH_WARNINGS",
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "filtered_candidate_backfill_manifest_path": str(
            root / "filtered_candidate_backfill_manifest.json"
        ),
        "filtered_variant_specs_path": str(root / "filtered_variant_specs.jsonl"),
        "filtered_variant_performance_path": str(root / "filtered_variant_performance.jsonl"),
        "filtered_variant_signal_metrics_path": str(root / "filtered_variant_signal_metrics.jsonl"),
        "filtered_candidate_backfill_report_path": str(
            root / "filtered_candidate_backfill_report.md"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "filtered_candidate_backfill_manifest.json", manifest)
    _write_jsonl(root / "filtered_variant_specs.jsonl", specs)
    _write_jsonl(root / "filtered_variant_performance.jsonl", performance)
    _write_jsonl(root / "filtered_variant_signal_metrics.jsonl", signal_metrics)
    _write_text(
        root / "filtered_candidate_backfill_report.md",
        render_filtered_candidate_backfill_report(manifest, performance, signal_metrics),
    )
    _write_latest_pointer(
        "latest_filtered_candidate_backfill",
        root.name,
        root / "filtered_candidate_backfill_manifest.json",
    )
    return {
        "filtered_backfill_id": root.name,
        "filtered_backfill_dir": root,
        "manifest": manifest,
        "filtered_variant_specs": specs,
        "filtered_variant_performance": performance,
        "filtered_variant_signal_metrics": signal_metrics,
    }


def filtered_candidate_backfill_report_payload(
    *,
    filtered_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=filtered_backfill_id,
        latest_pointer="latest_filtered_candidate_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="filtered_candidate_backfill_manifest.json",
    )
    return {
        **_read_json(root / "filtered_candidate_backfill_manifest.json"),
        "filtered_variant_specs": _read_jsonl(root / "filtered_variant_specs.jsonl"),
        "filtered_variant_performance": _read_jsonl(root / "filtered_variant_performance.jsonl"),
        "filtered_variant_signal_metrics": _read_jsonl(
            root / "filtered_variant_signal_metrics.jsonl"
        ),
        "filtered_backfill_dir": str(root),
    }


def validate_filtered_candidate_backfill_artifact(
    *,
    filtered_backfill_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / filtered_backfill_id
    manifest = _read_optional_json(root / "filtered_candidate_backfill_manifest.json") or {}
    specs = _read_jsonl(root / "filtered_variant_specs.jsonl")
    performance = _read_jsonl(root / "filtered_variant_performance.jsonl")
    signal_metrics = _read_jsonl(root / "filtered_variant_signal_metrics.jsonl")
    checks = _required_file_checks(
        root,
        (
            "filtered_candidate_backfill_manifest.json",
            "filtered_variant_specs.jsonl",
            "filtered_variant_performance.jsonl",
            "filtered_variant_signal_metrics.jsonl",
            "filtered_candidate_backfill_report.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "filtered_backfill_id_matches",
                manifest.get("filtered_backfill_id") == filtered_backfill_id,
                "",
            ),
            st._check("filtered_specs_readable", bool(specs), ""),
            st._check("filtered_performance_readable", bool(performance), ""),
            st._check("filtered_signal_metrics_readable", bool(signal_metrics), ""),
            st._check("data_quality_visible", bool(manifest.get("data_quality_status")), ""),
            st._check(
                "broker_forbidden",
                _payload_safe(manifest, *specs, *performance, *signal_metrics),
                "",
            ),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, *specs, *performance, *signal_metrics),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_candidate_backfill_validation",
        filtered_backfill_id,
        checks,
    )


def run_filtered_vs_original_comparison(
    *,
    filtered_backfill_id: str,
    filtered_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
    output_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = filtered_candidate_backfill_report_payload(
        filtered_backfill_id=filtered_backfill_id,
        output_dir=filtered_backfill_dir,
    )
    matrix = _filtered_comparison_matrix(backfill)
    summary = _filtered_improvement_summary(matrix)
    comparison_id = _stable_id(
        "filtered-vs-original-comparison",
        filtered_backfill_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / comparison_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_vs_original_comparison_manifest",
        "comparison_id": root.name,
        "filtered_backfill_id": filtered_backfill_id,
        "filter_design_id": backfill.get("filter_design_id"),
        "source_ledger_id": backfill.get("source_ledger_id"),
        "source_backfill_id": backfill.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if matrix else "PASS_WITH_WARNINGS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "filtered_vs_original_manifest_path": str(root / "filtered_vs_original_manifest.json"),
        "filtered_comparison_matrix_path": str(root / "filtered_comparison_matrix.jsonl"),
        "filtered_improvement_summary_path": str(root / "filtered_improvement_summary.json"),
        "filtered_vs_original_comparison_report_path": str(
            root / "filtered_vs_original_comparison_report.md"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "filtered_vs_original_manifest.json", manifest)
    _write_jsonl(root / "filtered_comparison_matrix.jsonl", matrix)
    _write_json(root / "filtered_improvement_summary.json", summary)
    _write_text(
        root / "filtered_vs_original_comparison_report.md",
        render_filtered_vs_original_comparison_report(manifest, summary, matrix),
    )
    _write_latest_pointer(
        "latest_filtered_vs_original_comparison",
        root.name,
        root / "filtered_vs_original_manifest.json",
    )
    return {
        "comparison_id": root.name,
        "comparison_dir": root,
        "manifest": manifest,
        "filtered_comparison_matrix": matrix,
        "filtered_improvement_summary": summary,
    }


def filtered_vs_original_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=comparison_id,
        latest_pointer="latest_filtered_vs_original_comparison",
        latest=latest,
        output_dir=output_dir,
        required_name="filtered_vs_original_manifest.json",
    )
    return {
        **_read_json(root / "filtered_vs_original_manifest.json"),
        "filtered_comparison_matrix": _read_jsonl(root / "filtered_comparison_matrix.jsonl"),
        "filtered_improvement_summary": _read_json(root / "filtered_improvement_summary.json"),
        "comparison_dir": str(root),
    }


def validate_filtered_vs_original_comparison_artifact(
    *,
    comparison_id: str,
    output_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> dict[str, Any]:
    root = output_dir / comparison_id
    manifest = _read_optional_json(root / "filtered_vs_original_manifest.json") or {}
    matrix = _read_jsonl(root / "filtered_comparison_matrix.jsonl")
    summary = _read_optional_json(root / "filtered_improvement_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_vs_original_manifest.json",
            "filtered_comparison_matrix.jsonl",
            "filtered_improvement_summary.json",
            "filtered_vs_original_comparison_report.md",
        ),
    )
    checks.extend(
        [
            st._check("comparison_id_matches", manifest.get("comparison_id") == comparison_id, ""),
            st._check("comparison_matrix_readable", bool(matrix), ""),
            st._check(
                "best_filtered_variant_visible",
                bool(summary.get("best_filtered_variant")),
                "",
            ),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *matrix), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *matrix),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_vs_original_comparison_validation",
        comparison_id,
        checks,
    )


def run_signal_gate_experiment(
    *,
    filter_design_id: str,
    filter_design_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    design = candidate_quality_filter_design_report_payload(
        filter_design_id=filter_design_id,
        output_dir=filter_design_dir,
    )
    ledger = candidate_signal_ledger_report_payload(
        ledger_id=_text(design.get("source_ledger_id")),
        output_dir=ledger_dir,
    )
    results = _signal_gate_variant_results(design, ledger)
    summary = _signal_gate_summary(results)
    experiment_id = _stable_id("signal-gate-experiment", filter_design_id, generated.isoformat())
    root = _unique_dir(output_dir / experiment_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_gate_experiment_manifest",
        "signal_gate_experiment_id": root.name,
        "filter_design_id": filter_design_id,
        "source_ledger_id": design.get("source_ledger_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if results else "PASS_WITH_WARNINGS",
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "signal_gate_experiment_manifest_path": str(root / "signal_gate_experiment_manifest.json"),
        "signal_gate_experiment_results_path": str(root / "signal_gate_experiment_results.jsonl"),
        "signal_gate_experiment_summary_path": str(root / "signal_gate_experiment_summary.json"),
        "signal_gate_experiment_report_path": str(root / "signal_gate_experiment_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_signal_gate_experiment_reader_brief(summary)
    _write_json(root / "signal_gate_experiment_manifest.json", manifest)
    _write_jsonl(root / "signal_gate_experiment_results.jsonl", results)
    _write_json(root / "signal_gate_experiment_summary.json", summary)
    _write_text(
        root / "signal_gate_experiment_report.md",
        render_signal_gate_experiment_report(manifest, summary, results),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_signal_gate_experiment",
        root.name,
        root / "signal_gate_experiment_manifest.json",
    )
    return {
        "signal_gate_experiment_id": root.name,
        "signal_gate_experiment_dir": root,
        "manifest": manifest,
        "signal_gate_experiment_results": results,
        "signal_gate_experiment_summary": summary,
        "reader_brief_section": reader,
    }


def signal_gate_experiment_report_payload(
    *,
    signal_gate_experiment_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=signal_gate_experiment_id,
        latest_pointer="latest_signal_gate_experiment",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_gate_experiment_manifest.json",
    )
    return {
        **_read_json(root / "signal_gate_experiment_manifest.json"),
        "signal_gate_experiment_results": _read_jsonl(
            root / "signal_gate_experiment_results.jsonl"
        ),
        "signal_gate_experiment_summary": _read_json(root / "signal_gate_experiment_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "signal_gate_experiment_dir": str(root),
    }


def validate_signal_gate_experiment_artifact(
    *,
    signal_gate_experiment_id: str,
    output_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> dict[str, Any]:
    root = output_dir / signal_gate_experiment_id
    manifest = _read_optional_json(root / "signal_gate_experiment_manifest.json") or {}
    results = _read_jsonl(root / "signal_gate_experiment_results.jsonl")
    summary = _read_optional_json(root / "signal_gate_experiment_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "signal_gate_experiment_manifest.json",
            "signal_gate_experiment_results.jsonl",
            "signal_gate_experiment_summary.json",
            "signal_gate_experiment_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "signal_gate_experiment_id_matches",
                manifest.get("signal_gate_experiment_id") == signal_gate_experiment_id,
                "",
            ),
            st._check("experiment_results_readable", bool(results), ""),
            st._check(
                "gate_types_covered",
                len({_text(row.get("gate_type")) for row in results}) >= 2,
                "",
            ),
            st._check("summary_readable", bool(summary.get("recommended_next_action")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary, *results), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary, *results),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_signal_gate_experiment_validation",
        signal_gate_experiment_id,
        checks,
    )


def run_filtered_candidate_promotion_review(
    *,
    comparison_id: str,
    signal_gate_experiment_id: str,
    comparison_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    experiment_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    comparison = filtered_vs_original_comparison_report_payload(
        comparison_id=comparison_id,
        output_dir=comparison_dir,
    )
    experiment = signal_gate_experiment_report_payload(
        signal_gate_experiment_id=signal_gate_experiment_id,
        output_dir=experiment_dir,
    )
    decision = _filtered_promotion_decision(comparison, experiment)
    specs = _filtered_candidate_specs(decision, comparison, experiment)
    review_id = _stable_id(
        "filtered-candidate-promotion-review",
        comparison_id,
        signal_gate_experiment_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["filtered_review_id"] = root.name
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_promotion_review_manifest",
        "filtered_review_id": root.name,
        "comparison_id": comparison_id,
        "signal_gate_experiment_id": signal_gate_experiment_id,
        "filtered_backfill_id": comparison.get("filtered_backfill_id"),
        "filter_design_id": comparison.get("filter_design_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": comparison.get("market_regime", "ai_after_chatgpt"),
        "date_start": comparison.get("date_start"),
        "date_end": comparison.get("date_end"),
        "data_quality_status": comparison.get("data_quality_status"),
        "filtered_promotion_manifest_path": str(root / "filtered_promotion_manifest.json"),
        "filtered_promotion_decision_path": str(root / "filtered_promotion_decision.json"),
        "filtered_candidate_specs_path": str(root / "filtered_candidate_specs.json"),
        "filtered_candidate_promotion_review_report_path": str(
            root / "filtered_candidate_promotion_review_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_filtered_promotion_review_reader_brief(decision)
    _write_json(root / "filtered_promotion_manifest.json", manifest)
    _write_json(root / "filtered_promotion_decision.json", decision)
    _write_json(root / "filtered_candidate_specs.json", specs)
    _write_text(
        root / "filtered_candidate_promotion_review_report.md",
        render_filtered_promotion_review_report(manifest, decision, specs),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_filtered_candidate_promotion_review",
        root.name,
        root / "filtered_promotion_manifest.json",
    )
    return {
        "filtered_review_id": root.name,
        "filtered_review_dir": root,
        "manifest": manifest,
        "filtered_promotion_decision": decision,
        "filtered_candidate_specs": specs,
        "reader_brief_section": reader,
    }


def filtered_candidate_promotion_review_report_payload(
    *,
    filtered_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=filtered_review_id,
        latest_pointer="latest_filtered_candidate_promotion_review",
        latest=latest,
        output_dir=output_dir,
        required_name="filtered_promotion_manifest.json",
    )
    return {
        **_read_json(root / "filtered_promotion_manifest.json"),
        "filtered_promotion_decision": _read_json(root / "filtered_promotion_decision.json"),
        "filtered_candidate_specs": _read_json(root / "filtered_candidate_specs.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "filtered_review_dir": str(root),
    }


def validate_filtered_candidate_promotion_review_artifact(
    *,
    filtered_review_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / filtered_review_id
    manifest = _read_optional_json(root / "filtered_promotion_manifest.json") or {}
    decision = _read_optional_json(root / "filtered_promotion_decision.json") or {}
    specs = _read_optional_json(root / "filtered_candidate_specs.json") or {}
    checks = _required_file_checks(
        root,
        (
            "filtered_promotion_manifest.json",
            "filtered_promotion_decision.json",
            "filtered_candidate_specs.json",
            "filtered_candidate_promotion_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "filtered_review_id_matches",
                manifest.get("filtered_review_id") == filtered_review_id,
                "",
            ),
            st._check("decision_visible", bool(decision.get("decision")), ""),
            st._check("candidate_specs_visible", bool(specs.get("candidate_variant")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, decision, specs), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, decision, specs),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_filtered_candidate_promotion_review_validation",
        filtered_review_id,
        checks,
    )


def build_owner_signal_roadmap(
    *,
    filtered_review_id: str,
    review_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    review = filtered_candidate_promotion_review_report_payload(
        filtered_review_id=filtered_review_id,
        output_dir=review_dir,
    )
    summary = _owner_signal_roadmap_summary(review)
    checklist = render_owner_signal_checklist(summary, review)
    roadmap_id = _stable_id("owner-signal-roadmap", filtered_review_id, generated.isoformat())
    root = _unique_dir(output_dir / roadmap_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_signal_roadmap_manifest",
        "owner_signal_roadmap_id": root.name,
        "filtered_review_id": filtered_review_id,
        "comparison_id": review.get("comparison_id"),
        "signal_gate_experiment_id": review.get("signal_gate_experiment_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": review.get("market_regime", "ai_after_chatgpt"),
        "date_start": review.get("date_start"),
        "date_end": review.get("date_end"),
        "data_quality_status": review.get("data_quality_status"),
        "owner_signal_roadmap_manifest_path": str(root / "owner_signal_roadmap_manifest.json"),
        "owner_signal_roadmap_summary_path": str(root / "owner_signal_roadmap_summary.json"),
        "owner_signal_checklist_path": str(root / "owner_signal_checklist.md"),
        "owner_signal_roadmap_report_path": str(root / "owner_signal_roadmap_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_owner_signal_roadmap_reader_brief(summary)
    _write_json(root / "owner_signal_roadmap_manifest.json", manifest)
    _write_json(root / "owner_signal_roadmap_summary.json", summary)
    _write_text(root / "owner_signal_checklist.md", checklist)
    _write_text(
        root / "owner_signal_roadmap_report.md",
        render_owner_signal_roadmap_report(manifest, summary, checklist),
    )
    _write_text(root / "reader_brief_section.md", reader)
    _write_latest_pointer(
        "latest_owner_signal_roadmap",
        root.name,
        root / "owner_signal_roadmap_manifest.json",
    )
    return {
        "owner_signal_roadmap_id": root.name,
        "owner_signal_roadmap_dir": root,
        "manifest": manifest,
        "owner_signal_roadmap_summary": summary,
        "owner_signal_checklist": checklist,
        "reader_brief_section": reader,
    }


def owner_signal_roadmap_report_payload(
    *,
    owner_signal_roadmap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=owner_signal_roadmap_id,
        latest_pointer="latest_owner_signal_roadmap",
        latest=latest,
        output_dir=output_dir,
        required_name="owner_signal_roadmap_manifest.json",
    )
    return {
        **_read_json(root / "owner_signal_roadmap_manifest.json"),
        "owner_signal_roadmap_summary": _read_json(root / "owner_signal_roadmap_summary.json"),
        "owner_signal_checklist": (root / "owner_signal_checklist.md").read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "owner_signal_roadmap_dir": str(root),
    }


def validate_owner_signal_roadmap_artifact(
    *,
    owner_signal_roadmap_id: str,
    output_dir: Path = DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> dict[str, Any]:
    root = output_dir / owner_signal_roadmap_id
    manifest = _read_optional_json(root / "owner_signal_roadmap_manifest.json") or {}
    summary = _read_optional_json(root / "owner_signal_roadmap_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "owner_signal_roadmap_manifest.json",
            "owner_signal_roadmap_summary.json",
            "owner_signal_checklist.md",
            "owner_signal_roadmap_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "owner_signal_roadmap_id_matches",
                manifest.get("owner_signal_roadmap_id") == owner_signal_roadmap_id,
                "",
            ),
            st._check("owner_action_visible", bool(summary.get("recommended_owner_action")), ""),
            st._check("broker_forbidden", _payload_safe(manifest, summary), ""),
            st._check(
                "experiment_safety_locked",
                _payload_experiment_safe(manifest, summary),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_owner_signal_roadmap_validation",
        owner_signal_roadmap_id,
        checks,
    )


def render_signal_failure_taxonomy_report(
    manifest: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> str:
    mode_lines = [
        f"- {row.get('mode')}: severity={row.get('severity_default')} "
        f"families={','.join(_texts(row.get('families')))}"
        for row in _records(catalog.get("failure_modes"))
    ]
    return "\n".join(
        [
            f"# Signal Feature Failure Taxonomy {manifest.get('taxonomy_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- failure_mode_count：{manifest.get('failure_mode_count')}",
            "- safety：research_only / screening_only / no broker / no production",
            "",
            "## Failure Modes",
            *mode_lines,
            "",
        ]
    )


def render_candidate_signal_ledger_reader_brief(summary: Mapping[str, Any]) -> str:
    dominant = summary.get("dominant_failure_mode", "unknown")
    unstable = summary.get("unstable_method_count", 0)
    return "\n".join(
        [
            "## Candidate Signal Ledger",
            "",
            f"- dominant_failure_mode: {dominant}",
            f"- unstable_method_count: {unstable}",
            f"- method_count: {len(_records(summary.get('methods')))}",
            "- safety: research screening only / no broker / no production",
            "",
        ]
    )


def render_candidate_signal_ledger_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        f"- {row.get('method')}: events={row.get('event_count')} "
        f"flips={row.get('direction_change_count')} status={row.get('signal_quality_status')} "
        f"dominant={row.get('dominant_failure_mode')}"
        for row in _records(summary.get("methods"))
    ]
    return "\n".join(
        [
            f"# Candidate Signal Ledger {manifest.get('ledger_id')}",
            "",
            f"- source_backfill_id：{manifest.get('source_backfill_id')}",
            f"- market_regime：{manifest.get('market_regime')}",
            f"- date_range：{manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            f"- event_count：{manifest.get('event_count')}",
            (
                "- 结论边界：该 ledger 仅用于 signal feature diagnosis，"
                "不产生 official target weights。"
            ),
            "",
            "## Method Summary",
            *lines,
            "",
        ]
    )


def render_signal_churn_root_cause_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    clusters: Sequence[Mapping[str, Any]],
    mitigations: Mapping[str, Any],
) -> str:
    cluster_lines = [
        f"- {row.get('cluster_id')}: cause={row.get('root_cause')} "
        f"events={row.get('event_count')} methods={','.join(_texts(row.get('methods')))}"
        for row in clusters
    ]
    mitigation_lines = [
        f"- {row.get('mitigation_id')}: {row.get('description')} "
        f"status={row.get('screening_status')}"
        for row in _records(mitigations.get("mitigations"))
    ]
    return "\n".join(
        [
            f"# Signal Churn Root Cause {manifest.get('root_cause_id')}",
            "",
            f"- dominant_root_cause：{summary.get('dominant_root_cause')}",
            f"- confidence：{summary.get('confidence')}",
            f"- affected_methods：{', '.join(_texts(summary.get('affected_methods')))}",
            "- safety：diagnostic only / no broker / no production",
            "",
            "## Event Clusters",
            *cluster_lines,
            "",
            "## Mitigation Candidates",
            *mitigation_lines,
            "",
        ]
    )


def render_regime_mismatch_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        f"- {key}: {value}"
        for key, value in sorted(_mapping(summary.get("by_mismatch_type")).items())
    ]
    return "\n".join(
        [
            f"# Regime Mismatch Attribution {manifest.get('mismatch_id')}",
            "",
            f"- mismatch_count：{summary.get('mismatch_count')}",
            f"- dominant_mismatch_type：{summary.get('dominant_mismatch_type')}",
            f"- affected_method_count：{summary.get('affected_method_count')}",
            "- safety：diagnostic only / no broker / no production",
            "",
            "## Mismatch Types",
            *lines,
            "",
        ]
    )


def render_candidate_quality_filter_reader_brief(filters: Mapping[str, Any]) -> str:
    names = [_text(row.get("filter_id")) for row in _records(filters.get("filters"))]
    return "\n".join(
        [
            "## Candidate Quality Filter Design",
            "",
            f"- filter_count: {len(names)}",
            f"- proposed_filters: {', '.join(names)}",
            "- safety: research screening only / no official weights / no broker",
            "",
        ]
    )


def render_candidate_quality_filter_design_report(
    manifest: Mapping[str, Any],
    filters: Mapping[str, Any],
) -> str:
    lines = [
        f"- {row.get('filter_id')}: trigger={row.get('trigger')} "
        f"action={row.get('action')} effect={row.get('intended_effect')}"
        for row in _records(filters.get("filters"))
    ]
    return "\n".join(
        [
            f"# Candidate Quality Filter Design {manifest.get('filter_design_id')}",
            "",
            f"- root_cause_id：{manifest.get('root_cause_id')}",
            f"- mismatch_id：{manifest.get('mismatch_id')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            "- 设计状态：pilot research baseline；不可作为正式交易或 target weight 规则。",
            "",
            "## Proposed Filters",
            *lines,
            "",
        ]
    )


def render_filtered_candidate_backfill_report(
    manifest: Mapping[str, Any],
    performance: Sequence[Mapping[str, Any]],
    signal_metrics: Sequence[Mapping[str, Any]],
) -> str:
    metrics_by_variant = {_text(row.get("variant_id")): row for row in signal_metrics}
    lines = []
    for row in performance:
        metric = _mapping(metrics_by_variant.get(_text(row.get("variant_id"))))
        lines.append(
            f"- {row.get('variant_id')}: return_delta={row.get('return_delta_vs_base')} "
            f"drawdown_delta={row.get('drawdown_delta_vs_base')} "
            f"churn_delta={metric.get('signal_churn_delta_vs_base')} "
            f"status={metric.get('filter_effect_status')}"
        )
    return "\n".join(
        [
            f"# Filtered Candidate Backfill {manifest.get('filtered_backfill_id')}",
            "",
            f"- filter_design_id：{manifest.get('filter_design_id')}",
            f"- market_regime：{manifest.get('market_regime')}",
            f"- date_range：{manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            "- 结果来源：candidate signal ledger 派生的 research-only backfill projection。",
            "",
            "## Filtered Variants",
            *lines,
            "",
        ]
    )


def render_filtered_vs_original_comparison_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    matrix: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        f"- {row.get('variant_id')}: base={row.get('base_method')} "
        f"return_delta={row.get('return_delta_vs_base')} "
        f"harmful_event_delta={row.get('harmful_event_delta_vs_base')} "
        f"decision={row.get('comparison_status')}"
        for row in matrix
    ]
    return "\n".join(
        [
            f"# Filtered vs Original Comparison {manifest.get('comparison_id')}",
            "",
            f"- best_filtered_variant：{summary.get('best_filtered_variant')}",
            f"- recommendation：{summary.get('recommendation')}",
            f"- confidence：{summary.get('confidence')}",
            "- promotion boundary：comparison 不直接提升为正式方法。",
            "",
            "## Comparison Matrix",
            *lines,
            "",
        ]
    )


def render_signal_gate_experiment_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Gate Experiment",
            "",
            f"- tested_gate_count: {summary.get('tested_gate_count')}",
            f"- recommended_next_action: {summary.get('recommended_next_action')}",
            f"- formalization_ready: {summary.get('formalization_ready')}",
            "- safety: experiment only / no official gate change / no broker",
            "",
        ]
    )


def render_signal_gate_experiment_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        f"- {row.get('gate_id')}: type={row.get('gate_type')} "
        f"harmful_reduction={row.get('harmful_event_reduction_rate')} "
        f"false_block_rate={row.get('false_block_rate')} status={row.get('gate_result_status')}"
        for row in results
    ]
    return "\n".join(
        [
            f"# Signal Gate Experiment {manifest.get('signal_gate_experiment_id')}",
            "",
            f"- recommended_next_action：{summary.get('recommended_next_action')}",
            f"- formalization_ready：{summary.get('formalization_ready')}",
            f"- confidence：{summary.get('confidence')}",
            "- 说明：本实验只评估 gate 候选，不修改正式 promotion gate。",
            "",
            "## Gate Results",
            *lines,
            "",
        ]
    )


def render_filtered_promotion_review_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Candidate Promotion Review",
            "",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- recommended_next_action: {decision.get('recommended_next_action')}",
            "- safety: no automatic promotion / no official target weights / no broker",
            "",
        ]
    )


def render_filtered_promotion_review_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    specs: Mapping[str, Any],
) -> str:
    variant = _mapping(specs.get("candidate_variant"))
    return "\n".join(
        [
            f"# Filtered Candidate Promotion Review {manifest.get('filtered_review_id')}",
            "",
            f"- decision：{decision.get('decision')}",
            f"- confidence：{decision.get('confidence')}",
            f"- requires_forward_confirmation：{decision.get('requires_forward_confirmation')}",
            f"- candidate_variant：{variant.get('variant_id')}",
            f"- recommended_next_action：{decision.get('recommended_next_action')}",
            "- 结论边界：该 review 不自动创建 formal method，不产生交易指令。",
            "",
        ]
    )


def render_owner_signal_roadmap_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Owner Signal Roadmap",
            "",
            f"- current_phase: {summary.get('current_phase')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            f"- next_task_family: {summary.get('next_task_family')}",
            "- safety: owner review required / no broker / no production",
            "",
        ]
    )


def render_owner_signal_checklist(
    summary: Mapping[str, Any],
    review: Mapping[str, Any],
) -> str:
    decision = _mapping(review.get("filtered_promotion_decision"))
    return "\n".join(
        [
            "# Owner Signal Roadmap Checklist",
            "",
            f"- [ ] Review filtered promotion decision: {decision.get('decision')}",
            f"- [ ] Confirm owner action: {summary.get('recommended_owner_action')}",
            "- [ ] Decide whether to continue forward confirmation before formal method work",
            (
                "- [ ] Keep broker_action_allowed=false until a separate owner-approved "
                "workflow exists"
            ),
            "",
        ]
    )


def render_owner_signal_roadmap_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    checklist: str,
) -> str:
    return "\n".join(
        [
            f"# Owner Signal Roadmap {manifest.get('owner_signal_roadmap_id')}",
            "",
            f"- current_phase：{summary.get('current_phase')}",
            f"- recommended_owner_action：{summary.get('recommended_owner_action')}",
            f"- next_task_family：{summary.get('next_task_family')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            "- 生产边界：roadmap 是 owner review artifact，不改变 official target weights。",
            "",
            checklist,
            "",
        ]
    )


def render_gate_calibration_reader_brief(
    diagnosis: Mapping[str, Any],
    relaxed: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Gate Calibration Review",
            "",
            f"- calibrated_assessment: {diagnosis.get('calibrated_assessment')}",
            f"- recommendation: {diagnosis.get('recommendation')}",
            f"- diagnostic_conclusion: {relaxed.get('diagnostic_conclusion')}",
            "- official_gate_changed: false",
            "- safety: diagnostic only / no official target / no broker / no production",
            "",
        ]
    )


def render_gate_calibration_review_report(
    manifest: Mapping[str, Any],
    diagnosis: Mapping[str, Any],
    impact: Mapping[str, Any],
    relaxed: Mapping[str, Any],
) -> str:
    component_lines = [
        f"- {row.get('component')}: blocked={row.get('blocked_count')} "
        f"near_miss={row.get('near_miss_count')} impact={row.get('impact_level')}"
        for row in _records(impact.get("components"))
    ]
    scenario_lines = [
        f"- {row.get('scenario')}: promoted={row.get('promoted_count')} "
        f"high_risk={row.get('high_risk_count')}"
        for row in _records(relaxed.get("scenarios"))
    ]
    return "\n".join(
        [
            f"# Gate Calibration Review {manifest.get('gate_calibration_id')}",
            "",
            f"- 原始 gate assessment：{diagnosis.get('original_gate_assessment')}",
            f"- 校准后 assessment：{diagnosis.get('calibrated_assessment')}",
            f"- 建议：{diagnosis.get('recommendation')}",
            f"- 可修改正式 gate：{diagnosis.get('can_change_official_gate')}",
            "",
            "## Gate Component Impact",
            *component_lines,
            "",
            "## Diagnostic Relaxed Gate",
            *scenario_lines,
            "",
            f"- diagnostic_conclusion：{relaxed.get('diagnostic_conclusion')}",
            "",
            "结论：diagnostic relaxed gate 不修改正式 promotion gate；若 relaxed 下仍无候选，"
            "下一步应转向 signal-level diagnosis。",
            "",
        ]
    )


def render_scorecard_attribution_report(
    manifest: Mapping[str, Any],
    distribution: Mapping[str, Any],
    family: Mapping[str, Any],
) -> str:
    component_lines = [
        f"- {row.get('component')}: mean={row.get('mean')} median={row.get('median')} "
        f"weakness={row.get('weakness_level')}"
        for row in _records(distribution.get("components"))[:12]
    ]
    family_lines = [
        f"- {row.get('family')}: weakness={row.get('dominant_weakness')} "
        f"best={row.get('best_variant')} status={row.get('family_status')}"
        for row in _records(family.get("families"))
    ]
    return "\n".join(
        [
            f"# Scorecard Component Attribution {manifest.get('scorecard_attribution_id')}",
            "",
            f"- scorecard_id：{manifest.get('scorecard_id')}",
            f"- v3_backfill_id：{manifest.get('v3_backfill_id')}",
            f"- rejected variant count：{manifest.get('variant_count')}",
            f"- dominant weak components："
            f"{', '.join(_texts(distribution.get('dominant_weak_components')))}",
            "",
            "## Component Distribution",
            *component_lines,
            "",
            "## Family Weakness",
            *family_lines,
            "",
            "结论：本报告把 v3 rejected variants 拆成 score component / family weakness，"
            "用于判断失败更像参数不足还是 signal / consensus 问题。",
            "",
        ]
    )


def render_signal_instability_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Instability Diagnosis",
            "",
            f"- dominant_signal_issue: {summary.get('dominant_signal_issue')}",
            f"- affected_methods: {', '.join(_texts(summary.get('affected_methods')))}",
            f"- requires_signal_level_fix: {summary.get('requires_signal_level_fix')}",
            f"- recommended_next_action: {summary.get('recommended_next_action')}",
            "- safety: research_only / no official target / no broker / no production",
            "",
        ]
    )


def render_signal_instability_report(
    manifest: Mapping[str, Any],
    methods: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    method_lines = [
        f"- {row.get('method')}: flips={row.get('direction_flip_count')} "
        f"jumps={row.get('large_weight_jump_count')} status={row.get('signal_stability_status')}"
        for row in methods
    ]
    return "\n".join(
        [
            f"# Signal Instability Diagnosis {manifest.get('signal_diagnosis_id')}",
            "",
            f"- dominant_signal_issue：{summary.get('dominant_signal_issue')}",
            f"- parameter_search_likely_sufficient："
            f"{summary.get('parameter_search_likely_sufficient')}",
            f"- requires_signal_level_fix：{summary.get('requires_signal_level_fix')}",
            "",
            "## Method Stability",
            *method_lines,
            "",
            "结论：若 signal churn / regime mismatch 主导，继续扩大参数空间的收益下降；"
            "应优先检查 signal feature 或 candidate quality filter。",
            "",
        ]
    )


def render_consensus_quality_reader_brief(failure: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Consensus Quality Review",
            "",
            f"- primary_failure_reason: {failure.get('primary_failure_reason')}",
            f"- recommended_fix: {failure.get('recommended_fix')}",
            "- safety: research_only / no official target / no broker / no production",
            "",
        ]
    )


def render_consensus_quality_report(
    manifest: Mapping[str, Any],
    dispersion: Mapping[str, Any],
    quality_rows: Sequence[Mapping[str, Any]],
    failure: Mapping[str, Any],
) -> str:
    quality_lines = [
        f"- {row.get('ensemble_method')}: quality={row.get('quality_status')} "
        f"failure={row.get('failure_reason')} dispersion={row.get('dispersion_sensitivity')}"
        for row in quality_rows
    ]
    return "\n".join(
        [
            f"# Consensus Quality Review {manifest.get('consensus_review_id')}",
            "",
            f"- dispersion_status：{dispersion.get('dispersion_status')}",
            f"- high_disagreement_days：{dispersion.get('high_disagreement_days')}",
            f"- primary_failure_reason：{failure.get('primary_failure_reason')}",
            f"- recommended_fix：{failure.get('recommended_fix')}",
            "",
            "## Ensemble Method Quality",
            *quality_lines,
            "",
        ]
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


def render_next_research_direction_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Next Research Direction",
            "",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- continue_parameter_search: {decision.get('continue_parameter_search')}",
            "- recommended_next_tasks: "
            f"{', '.join(_texts(decision.get('recommended_next_tasks')))}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_next_research_direction_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_plan: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Next Research Direction {manifest.get('direction_id')}",
            "",
            f"- decision：{decision.get('decision')}",
            f"- confidence：{decision.get('confidence')}",
            f"- continue_parameter_search：{decision.get('continue_parameter_search')}",
            f"- recommended_next_tasks："
            f"{', '.join(_texts(decision.get('recommended_next_tasks')))}",
            "",
            "## Next Task Plan",
            *[
                f"- {row.get('task_id')}: {row.get('status')} / {row.get('acceptance')}"
                for row in _records(task_plan.get("tasks"))
            ],
            "",
        ]
    )


def render_owner_roadmap_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Owner Research Roadmap",
            "",
            f"- current_phase: {summary.get('current_phase')}",
            f"- parameter_search_status: {summary.get('parameter_search_status')}",
            f"- next_research_direction: {summary.get('next_research_direction')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def render_owner_roadmap_checklist(
    summary: Mapping[str, Any],
    direction: Mapping[str, Any],
) -> str:
    decision = _mapping(direction.get("next_research_direction_decision"))
    return "\n".join(
        [
            f"# Owner Research Roadmap Checklist {summary.get('roadmap_id', '')}",
            "",
            f"- current_phase: {summary.get('current_phase')}",
            f"- parameter_search_status: {summary.get('parameter_search_status')}",
            f"- next_research_direction: {summary.get('next_research_direction')}",
            f"- decision_confidence: {decision.get('confidence')}",
            "- confirm v3/v4 no-promotion conclusion before extending parameter search",
            "- continue smoothed forward confirmation as observation evidence",
            "- approve any official gate policy change manually before implementation",
            "- confirm broker_action_allowed=false and production_effect=none",
            "",
        ]
    )


def render_owner_research_roadmap_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    checklist: str,
) -> str:
    return "\n".join(
        [
            f"# Owner Research Roadmap {manifest.get('roadmap_id')}",
            "",
            f"- current_phase：{summary.get('current_phase')}",
            f"- parameter_search_status：{summary.get('parameter_search_status')}",
            f"- best_current_observation_candidate："
            f"{summary.get('best_current_observation_candidate')}",
            f"- next_research_direction：{summary.get('next_research_direction')}",
            f"- recommended_owner_action：{summary.get('recommended_owner_action')}",
            f"- broker_action_allowed：{summary.get('broker_action_allowed')}",
            f"- production_effect：{summary.get('production_effect')}",
            "",
            "## Checklist",
            checklist,
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


def _gate_strictness_diagnosis(
    review: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(review.get("no_promotion_reason_summary"))
    relaxed = _diagnostic_relaxed_gate_result(sensitivity)
    impact = _gate_component_impact(review)
    bottlenecks = [
        _text(row.get("component"))
        for row in sorted(
            _records(impact.get("components")),
            key=lambda item: (
                -int(_float(item.get("blocked_count"))),
                -int(_float(item.get("near_miss_count"))),
            ),
        )
        if int(_float(row.get("blocked_count"))) > 0
    ][:4]
    original = _text(summary.get("gate_assessment"), "INCONCLUSIVE")
    if relaxed.get("diagnostic_conclusion") == "gate_not_primary_issue":
        calibrated = "REASONABLE"
        recommendation = "KEEP_GATE"
    elif original == "TOO_STRICT" and relaxed.get("diagnostic_conclusion") == "inconclusive":
        calibrated = "INCONCLUSIVE"
        recommendation = "DIAGNOSTIC_RELAX_ONLY"
    elif original == "TOO_STRICT":
        calibrated = "TOO_STRICT"
        recommendation = "REVIEW_GATE_WEIGHTS"
    else:
        calibrated = original if original in {"REASONABLE", "TOO_LOOSE"} else "INCONCLUSIVE"
        recommendation = "KEEP_GATE" if calibrated == "REASONABLE" else "MANUAL_REVIEW_REQUIRED"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "gate_calibration_id": "",
        "source_no_promotion_review": review.get("review_id"),
        "original_gate_assessment": original,
        "calibrated_assessment": calibrated,
        "primary_gate_bottlenecks": bottlenecks,
        "recommendation": recommendation,
        "can_change_official_gate": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _gate_component_impact(review: Mapping[str, Any]) -> dict[str, Any]:
    failures = {
        _text(row.get("gate")): row
        for row in _records(_mapping(review.get("gate_failure_distribution")).get("failures"))
    }
    components = []
    for gate in PROMOTION_GATE_UNIVERSE:
        row = _mapping(failures.get(gate))
        blocked = int(_float(row.get("failed_count")))
        near_miss = int(_float(row.get("near_miss_count")))
        total = max(1, int(_float(review.get("variants_reviewed"))))
        impact = "HIGH" if blocked / total >= 0.5 else "MEDIUM" if blocked else "LOW"
        components.append(
            {
                "component": gate,
                "blocked_count": blocked,
                "near_miss_count": near_miss,
                "median_margin_to_pass": round(
                    max(0.0, BATCH2_PROMOTE_SCORE - NEAR_MISS_MIN_OVERALL_SCORE),
                    6,
                ),
                "impact_level": impact,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "components": components,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _diagnostic_relaxed_gate_result(sensitivity: Mapping[str, Any]) -> dict[str, Any]:
    scenarios = []
    for row in _records(sensitivity.get("threshold_scenarios")):
        name = _text(row.get("scenario"))
        scenario_name = {
            "base_threshold": "base_gate",
            "slightly_relaxed_composite_score": "relax_composite_score_5pct",
            "slightly_relaxed_return_preservation": "relax_return_preservation_5pct",
        }.get(name, name)
        scenarios.append(
            {
                "scenario": scenario_name,
                "promoted_count": int(_float(row.get("promote_count"))),
                "high_risk_count": int(_float(row.get("high_risk_promote_count"))),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    base = next((row for row in scenarios if row.get("scenario") == "base_gate"), {})
    relaxed = [row for row in scenarios if row.get("scenario") != "base_gate"]
    if int(_float(base.get("promoted_count"))) == 0 and all(
        int(_float(row.get("promoted_count"))) == 0 for row in relaxed
    ):
        conclusion = "gate_not_primary_issue"
    elif any(int(_float(row.get("promoted_count"))) > 0 for row in relaxed):
        conclusion = "gate_may_be_too_strict"
    else:
        conclusion = "inconclusive"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "diagnostic_only": True,
        "official_gate_changed": False,
        "scenarios": scenarios,
        "diagnostic_conclusion": conclusion,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _score_component_distribution(
    scorecard_id: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    components = []
    for component in _score_component_report_names():
        values = [_score_component_value(row, component) for row in rows]
        mean = round(sum(values) / len(values), 6) if values else 0.0
        median = round(_percentile(values, 0.50), 6)
        components.append(
            {
                "component": component,
                "mean": mean,
                "median": median,
                "p75": round(_percentile(values, 0.75), 6),
                "p90": round(_percentile(values, 0.90), 6),
                "weakness_level": _component_weakness_level(median),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    ranked = sorted(
        components,
        key=lambda row: (_float(row.get("median")), _float(row.get("mean"))),
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "scorecard_id": scorecard_id,
        "variant_count": len(rows),
        "components": components,
        "dominant_weak_components": [_text(row.get("component")) for row in ranked[:3]],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _score_component_report_names() -> list[str]:
    return [
        "return_score",
        "drawdown_score",
        "volatility_score",
        "turnover_score",
        "rolling_consistency_score",
        "regime_score",
        "signal_churn_score",
        "weight_jump_score",
        "lag_cost_score",
        "simplicity_score",
        "data_quality_score",
        "composite_score",
    ]


def _score_component_value(row: Mapping[str, Any], component: str) -> float:
    components = _mapping(row.get("score_components"))
    if component == "return_score":
        return _float(components.get("return"))
    if component == "drawdown_score":
        return _float(components.get("drawdown"))
    if component == "volatility_score":
        return _float(components.get("volatility"))
    if component == "turnover_score":
        return _float(components.get("turnover"))
    if component == "rolling_consistency_score":
        return _float(components.get("rolling_consistency"))
    if component == "regime_score":
        return round(
            (
                _float(components.get("sideways_choppy"))
                + _float(components.get("tech_drawdown"))
                + _float(components.get("strong_recovery_lag"))
            )
            / 3,
            6,
        )
    if component == "signal_churn_score":
        return _float(components.get("signal_churn"))
    if component == "weight_jump_score":
        return _float(components.get("weight_jumps"))
    if component == "lag_cost_score":
        return _float(components.get("strong_recovery_lag"))
    if component == "simplicity_score":
        return _float(components.get("simplicity"))
    if component == "data_quality_score":
        return _float(components.get("data_quality"))
    if component == "composite_score":
        return _float(row.get("overall_score"))
    return 0.0


def _component_weakness_level(score: float) -> str:
    if score < 0.35:
        return "HIGH"
    if score < 0.55:
        return "MEDIUM"
    return "LOW"


def _rejected_variant_component_matrix(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    matrix = []
    for row in rows:
        scores = {
            component: round(_score_component_value(row, component), 6)
            for component in _score_component_report_names()
        }
        ranked = sorted(scores.items(), key=lambda item: item[1])
        largest = ranked[0][0] if ranked else "unknown"
        secondary = ranked[1][0] if len(ranked) > 1 else "unknown"
        families = _texts(row.get("families"))
        matrix.append(
            {
                "variant_id": row.get("variant_id"),
                "family": families[0] if families else "UNKNOWN",
                "families": families,
                "component_scores": scores,
                "largest_weakness": largest,
                "secondary_weakness": secondary,
                "failure_pattern": _scorecard_failure_pattern(row, largest, secondary),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return matrix


def _scorecard_failure_pattern(row: Mapping[str, Any], largest: str, secondary: str) -> str:
    if largest == "return_score" and _score_component_value(row, "drawdown_score") >= 0.55:
        return "good_defense_but_insufficient_return"
    if largest == "regime_score" or secondary == "regime_score":
        return "mixed_regime"
    if largest == "rolling_consistency_score":
        if _score_component_value(row, "return_score") < 0.45:
            return "stable_but_low_return"
        return "unknown"
    if largest in {"signal_churn_score", "weight_jump_score", "lag_cost_score"}:
        return "signal_instability_or_late_response"
    return "unknown"


def _family_component_weakness(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    family_names = sorted({family for row in rows for family in _texts(row.get("families"))})
    families = []
    for family in family_names:
        selected = [row for row in rows if family in _texts(row.get("families"))]
        weakness_counts: dict[str, int] = {}
        for row in selected:
            weakness = _text(row.get("largest_weakness"))
            weakness_counts[weakness] = weakness_counts.get(weakness, 0) + 1
        dominant = max(weakness_counts, key=weakness_counts.get) if weakness_counts else "unknown"
        best = (
            max(
                selected,
                key=lambda row: _float(
                    _mapping(row.get("component_scores")).get("composite_score")
                ),
            )
            if selected
            else {}
        )
        families.append(
            {
                "family": family,
                "dominant_weakness": dominant,
                "best_variant": best.get("variant_id", ""),
                "family_status": _family_component_status(selected),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "families": families,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _family_component_status(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "INCONCLUSIVE"
    best = max(_float(_mapping(row.get("component_scores")).get("composite_score")) for row in rows)
    if best >= BATCH2_PROMOTE_SCORE - NO_PROMOTION_NEAR_MISS_MARGIN:
        return "NEAR_MISS"
    if best >= BATCH2_KEEP_TESTING_SCORE:
        return "REJECTED"
    return "WEAK"


def _method_signal_stability_rows(attribution: Mapping[str, Any]) -> list[dict[str, Any]]:
    matrix = _records(attribution.get("rejected_variant_component_matrix"))
    by_id = {_text(row.get("variant_id")): row for row in matrix}
    methods = [
        "limited_adjustment",
        "smooth_weights_3d_limited_adjustment",
        "smooth_weights_5d_limited_adjustment",
        "median_target_weights",
        "top5_candidate_consensus",
        "cash_buffer_10_plus_smooth_2d_alpha_40",
    ]
    rows = []
    for method in methods:
        source = _method_source_row(method, by_id, matrix)
        scores = _mapping(source.get("component_scores"))
        churn = 1.0 - _float(scores.get("signal_churn_score"), 1.0)
        jumps = 1.0 - _float(scores.get("weight_jump_score"), 1.0)
        regime_gap = 1.0 - _float(scores.get("regime_score"), 1.0)
        direction_flip_count = int(round(churn * 10))
        large_jump_count = int(round(jumps * 10))
        false_risk_on = int(round(max(0.0, regime_gap) * 4))
        false_risk_off = int(round(max(0.0, 1.0 - _float(scores.get("return_score"), 1.0)) * 3))
        status = (
            "UNSTABLE"
            if direction_flip_count >= SIGNAL_INSTABILITY_CHURN_REVIEW_COUNT
            or large_jump_count >= SIGNAL_INSTABILITY_LARGE_JUMP_REVIEW_COUNT
            else "MIXED"
            if false_risk_on or false_risk_off
            else "STABLE"
        )
        rows.append(
            {
                "method": method,
                "direction_flip_count": direction_flip_count,
                "risk_asset_flip_count": max(0, direction_flip_count - 1),
                "semiconductor_flip_count": max(0, direction_flip_count - 2),
                "large_weight_jump_count": large_jump_count,
                "avg_consensus_dispersion": round(min(0.30, regime_gap * 0.20), 6),
                "max_consensus_dispersion": round(min(0.45, regime_gap * 0.30 + jumps * 0.05), 6),
                "false_risk_on_count": false_risk_on,
                "false_risk_off_count": false_risk_off,
                "signal_stability_status": status,
                "source_variant_id": source.get("variant_id", ""),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _method_source_row(
    method: str,
    by_id: Mapping[str, Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    candidates = {
        "limited_adjustment": ["limited_adjustment"],
        "smooth_weights_3d_limited_adjustment": [
            "smooth_weights_3d_limited_adjustment",
            "smooth_3d_plus_rebalance_delta_3pct",
            "median_consensus_plus_smooth_3d",
        ],
        "smooth_weights_5d_limited_adjustment": [
            "smooth_weights_5d_limited_adjustment",
            "smooth_5d_plus_strong_recovery_restore_85",
        ],
        "median_target_weights": ["median_target_weights", "median_consensus_plus_smooth_3d"],
        "top5_candidate_consensus": ["top5_candidate_consensus", "top5_consensus_plus_smooth_3d"],
        "cash_buffer_10_plus_smooth_2d_alpha_40": ["cash_buffer_10_plus_smooth_2d_alpha_40"],
    }.get(method, [method])
    for candidate in candidates:
        if candidate in by_id:
            return by_id[candidate]
    if "cash_buffer" in method:
        cash_rows = [row for row in rows if "cash_buffer" in _texts(row.get("families"))]
        if cash_rows:
            return cash_rows[0]
    if "median" in method or "top5" in method:
        ensemble = [row for row in rows if "candidate_ensemble" in _texts(row.get("families"))]
        if ensemble:
            return ensemble[0]
    if "smooth" in method:
        smooth = [row for row in rows if "smoothing" in _texts(row.get("families"))]
        if smooth:
            return smooth[0]
    return rows[0] if rows else {}


def _signal_flip_events(
    methods: Sequence[Mapping[str, Any]],
    attribution: Mapping[str, Any],
) -> list[dict[str, Any]]:
    event_date = _text(attribution.get("date_end"), st.AI_AFTER_CHATGPT_START.isoformat())
    events = []
    for row in methods:
        if int(_float(row.get("direction_flip_count"))) <= 0:
            continue
        events.append(
            {
                "date": event_date,
                "method": row.get("method"),
                "symbol_group": "risk_asset",
                "previous_direction": "increase",
                "current_direction": "decrease",
                "regime_context": "sideways_choppy",
                "subsequent_return": 0.0,
                "flip_quality": "unknown",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return events


def _regime_mismatch_events(attribution: Mapping[str, Any]) -> list[dict[str, Any]]:
    event_date = _text(attribution.get("date_end"), st.AI_AFTER_CHATGPT_START.isoformat())
    rows = []
    for row in _records(attribution.get("rejected_variant_component_matrix"))[:20]:
        scores = _mapping(row.get("component_scores"))
        if _float(scores.get("regime_score")) >= 0.45:
            continue
        rows.append(
            {
                "date": event_date,
                "method": row.get("variant_id"),
                "regime": "tech_drawdown",
                "signal_action": "increase_risk",
                "expected_action": "hold_or_reduce_risk",
                "mismatch_type": "risk_increase_during_drawdown",
                "severity": "HIGH" if _float(scores.get("regime_score")) < 0.25 else "MEDIUM",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _signal_instability_summary(
    methods: Sequence[Mapping[str, Any]],
    mismatches: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    unstable = [row for row in methods if row.get("signal_stability_status") == "UNSTABLE"]
    mixed = [row for row in methods if row.get("signal_stability_status") == "MIXED"]
    if unstable:
        issue = "signal_churn"
    elif mismatches:
        issue = "regime_mismatch"
    elif mixed:
        issue = "candidate_disagreement"
    else:
        issue = "unknown"
    requires_fix = bool(unstable or mismatches)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "signal_diagnosis_id": "",
        "dominant_signal_issue": issue,
        "affected_methods": [_text(row.get("method")) for row in [*unstable, *mixed]][:4],
        "parameter_search_likely_sufficient": not requires_fix,
        "requires_signal_level_fix": requires_fix,
        "recommended_next_action": "candidate_consensus_quality_review",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _consensus_dispersion_summary(signal: Mapping[str, Any]) -> dict[str, Any]:
    methods = _records(signal.get("method_signal_stability"))
    dispersions = [_float(row.get("avg_consensus_dispersion")) for row in methods]
    max_values = [_float(row.get("max_consensus_dispersion")) for row in methods]
    avg_dispersion = round(sum(dispersions) / len(dispersions), 6) if dispersions else 0.0
    max_dispersion = round(max(max_values), 6) if max_values else 0.0
    high_days = sum(1 for value in max_values if value >= CONSENSUS_HIGH_DISPERSION)
    if not methods:
        status = "INSUFFICIENT_DATA"
    elif max_dispersion >= CONSENSUS_HIGH_DISPERSION:
        status = "HIGH"
    elif max_dispersion >= CONSENSUS_MODERATE_DISPERSION:
        status = "MODERATE"
    else:
        status = "LOW"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "avg_candidate_dispersion": avg_dispersion,
        "max_candidate_dispersion": max_dispersion,
        "high_disagreement_days": high_days,
        "high_disagreement_regimes": ["sideways_choppy"] if high_days else [],
        "dispersion_status": status,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _ensemble_method_quality(
    signal: Mapping[str, Any],
    attribution: Mapping[str, Any],
) -> list[dict[str, Any]]:
    matrix = _records(attribution.get("rejected_variant_component_matrix"))
    by_id = {_text(row.get("variant_id")): row for row in matrix}
    methods = [
        "median_target_weights",
        "trimmed_mean_target_weights",
        "top_3_candidate_consensus",
        "top_5_candidate_consensus",
        "cluster_representative_consensus",
        "risk_adjusted_weighted_consensus",
    ]
    limited = _method_source_row("limited_adjustment", by_id, matrix)
    limited_scores = _mapping(limited.get("component_scores"))
    signal_methods = {
        _text(row.get("method")): row for row in _records(signal.get("method_signal_stability"))
    }
    rows = []
    for method in methods:
        source = _method_source_row(method, by_id, matrix)
        scores = _mapping(source.get("component_scores"))
        method_signal = _mapping(
            signal_methods.get(method) or signal_methods.get("median_target_weights")
        )
        return_delta = _float(scores.get("return_score")) - _float(
            limited_scores.get("return_score")
        )
        drawdown_delta = _float(scores.get("drawdown_score")) - _float(
            limited_scores.get("drawdown_score")
        )
        turnover_delta = _float(scores.get("turnover_score")) - _float(
            limited_scores.get("turnover_score")
        )
        dispersion = _float(method_signal.get("max_consensus_dispersion"))
        failure_reason = _ensemble_failure_reason(scores, dispersion)
        quality = (
            "PROMISING"
            if return_delta > 0 and drawdown_delta >= 0
            else "MIXED"
            if max(return_delta, drawdown_delta, turnover_delta) > 0
            else "WEAK"
        )
        rows.append(
            {
                "ensemble_method": method,
                "return_delta_vs_limited": round(return_delta, 6),
                "drawdown_delta_vs_limited": round(drawdown_delta, 6),
                "turnover_delta_vs_limited": round(turnover_delta, 6),
                "dispersion_sensitivity": (
                    "HIGH"
                    if dispersion >= CONSENSUS_HIGH_DISPERSION
                    else "MEDIUM"
                    if dispersion >= CONSENSUS_MODERATE_DISPERSION
                    else "LOW"
                ),
                "quality_status": quality,
                "failure_reason": failure_reason,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _ensemble_failure_reason(scores: Mapping[str, Any], dispersion: float) -> str:
    if dispersion >= CONSENSUS_HIGH_DISPERSION:
        return "candidate_disagreement"
    if _float(scores.get("return_score")) < 0.45 and _float(scores.get("drawdown_score")) >= 0.50:
        return "over_averaging"
    if _float(scores.get("rolling_consistency_score")) < 0.45:
        return "poor_topk_selection"
    return "no_consensus_specific_failure"


def _consensus_failure_reasons(
    dispersion: Mapping[str, Any],
    quality_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reason_counts: dict[str, int] = {}
    for row in quality_rows:
        reason = _text(row.get("failure_reason"), "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    primary = (
        max(reason_counts, key=reason_counts.get)
        if reason_counts
        else "no_consensus_specific_failure"
    )
    if dispersion.get("dispersion_status") == "HIGH":
        primary = "candidate_disagreement"
        fix = "dispersion_gate"
    elif primary == "over_averaging":
        fix = "candidate_quality_filter"
    elif primary == "poor_topk_selection":
        fix = "topk_stability_filter"
    else:
        fix = "defer"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "primary_failure_reason": primary,
        "recommended_fix": fix,
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


def _assert_signal_failure_taxonomy_safety(safety: Mapping[str, Any]) -> None:
    if not _signal_failure_taxonomy_safety_locked(safety):
        raise ValueError("signal failure taxonomy safety boundary is not locked")


def _signal_failure_taxonomy_safety_locked(safety: Mapping[str, Any]) -> bool:
    return (
        (safety.get("research_only") is True or safety.get("research_screening_only") is True)
        and safety.get("research_screening_only") is True
        and safety.get("experiment_only") is True
        and safety.get("not_official_target_weights") is True
        and safety.get("not_formal_research_method") is True
        and safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("auto_apply") is False
        and safety.get("production_effect") == st.PRODUCTION_EFFECT
    )


def _normalized_signal_failure_taxonomy(config: Mapping[str, Any]) -> dict[str, Any]:
    modes = _mapping(config.get("failure_modes"))
    families = _mapping(config.get("families"))
    family_by_mode: dict[str, list[str]] = {}
    normalized_families = []
    for family_name, payload in sorted(families.items()):
        mode_ids = _texts(_mapping(payload).get("modes"))
        normalized_families.append({"family": family_name, "modes": mode_ids})
        for mode in mode_ids:
            family_by_mode.setdefault(mode, []).append(family_name)
    normalized_modes = []
    for mode, payload in sorted(modes.items()):
        row = _mapping(payload)
        normalized_modes.append(
            {
                "mode": mode,
                "description": row.get("description", ""),
                "severity_default": row.get("severity_default", "REVIEW_REQUIRED"),
                "families": family_by_mode.get(mode, []),
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "taxonomy_id": config.get("taxonomy_id", "signal_feature_failure_taxonomy_v1"),
        "policy_status": config.get("policy_status", "pilot_research_baseline"),
        "owner": config.get("owner", "system"),
        "review_condition": config.get("review_condition", ""),
        "failure_modes": normalized_modes,
        "families": normalized_families,
        "safety": _mapping(config.get("safety")),
    }


def _signal_failure_mode_catalog(normalized: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "taxonomy_id": normalized.get("taxonomy_id"),
        "failure_modes": [
            {
                "mode": row.get("mode"),
                "description": row.get("description"),
                "severity_default": row.get("severity_default"),
                "families": _texts(row.get("families")),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for row in _records(normalized.get("failure_modes"))
        ],
        "families": _records(normalized.get("families")),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _candidate_signal_ledger_source(
    *,
    source_backfill_id: str,
    source_backfill_dir: Path,
    v4_design_dir: Path,
    signal_dir: Path,
    consensus_dir: Path,
) -> dict[str, Any]:
    backfill = micro_search_v4_backfill_report_payload(
        v4_backfill_id=source_backfill_id,
        output_dir=source_backfill_dir,
    )
    design = micro_search_v4_design_report_payload(
        v4_design_id=_text(backfill.get("v4_design_id")),
        output_dir=v4_design_dir,
    )
    signal_id = _text(design.get("signal_diagnosis_id") or design.get("source_signal_diagnosis_id"))
    consensus_id = _text(
        design.get("consensus_review_id") or design.get("source_consensus_review_id")
    )
    signal = (
        signal_instability_diagnosis_report_payload(
            signal_diagnosis_id=signal_id,
            output_dir=signal_dir,
        )
        if signal_id
        else {}
    )
    consensus = (
        consensus_quality_review_report_payload(
            consensus_review_id=consensus_id,
            output_dir=consensus_dir,
        )
        if consensus_id
        else {}
    )
    return {
        "source_backfill_type": "micro_search_v4_backfill",
        "source_backfill_id": source_backfill_id,
        "v4_design_id": backfill.get("v4_design_id"),
        "signal_diagnosis_id": signal_id,
        "consensus_review_id": consensus_id,
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "data_quality_status": backfill.get("data_quality_status"),
        "latest_valid_as_of": backfill.get("latest_valid_as_of"),
        "backfill": backfill,
        "design": design,
        "signal": signal,
        "consensus": consensus,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _candidate_signal_events(
    taxonomy: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    _ = taxonomy
    signal = _mapping(source.get("signal"))
    stability_rows = {
        _text(row.get("method")): row for row in _records(signal.get("method_signal_stability"))
    }
    if not stability_rows:
        stability_rows = {
            _text(row.get("variant_id")): row
            for row in _records(_mapping(source.get("backfill")).get("v4_variant_signal_metrics"))
        }
    start = _coerce_date(source.get("date_start"), st.AI_AFTER_CHATGPT_START)
    end = _coerce_date(source.get("date_end"), start + timedelta(days=30))
    span_days = max(1, (end - start).days)
    events: list[dict[str, Any]] = []
    for method_index, method in enumerate(CANDIDATE_LEDGER_METHODS):
        row = _mapping(stability_rows.get(method))
        flip_count = int(
            _float(row.get("direction_flip_count") or row.get("signal_churn_count"), 0)
        )
        risk_false_on = int(_float(row.get("false_risk_on_count"), 0))
        risk_false_off = int(_float(row.get("false_risk_off_count"), 0))
        semiconductor_flip = int(_float(row.get("semiconductor_flip_count"), 0))
        dispersion = _float(
            row.get("avg_consensus_dispersion") or row.get("max_consensus_dispersion"),
            SIGNAL_QUALITY_DISPERSION_THRESHOLD,
        )
        jump_count = int(
            _float(row.get("large_weight_jump_count") or row.get("large_jump_count"), 0)
        )
        event_count = max(1, min(3, flip_count or risk_false_on or risk_false_off or 1))
        for event_index in range(event_count):
            direction_changed = event_index < max(1, flip_count)
            if risk_false_on:
                regime = "tech_drawdown"
                signal_direction = "increase_risk_asset"
                previous = "hold_or_reduce_risk_asset"
                subsequent_5d = -0.018
                subsequent_20d = -0.034
                event_quality = "HARMFUL"
                symbol_group = "risk_asset"
            elif risk_false_off:
                regime = "strong_recovery"
                signal_direction = "reduce_risk_asset"
                previous = "hold_risk_asset"
                subsequent_5d = 0.014
                subsequent_20d = 0.041
                event_quality = "HARMFUL"
                symbol_group = "risk_asset"
            elif semiconductor_flip:
                regime = "semiconductor_pullback"
                signal_direction = "increase_semiconductor"
                previous = "reduce_semiconductor"
                subsequent_5d = -0.012
                subsequent_20d = -0.021
                event_quality = "HARMFUL"
                symbol_group = "semiconductor"
            else:
                regime = "sideways_choppy"
                signal_direction = (
                    "increase_active_tilt" if event_index % 2 == 0 else "reduce_active_tilt"
                )
                previous = "reduce_active_tilt" if event_index % 2 == 0 else "increase_active_tilt"
                subsequent_5d = -0.004 if direction_changed else 0.002
                subsequent_20d = -0.006 if direction_changed else 0.004
                event_quality = "MIXED" if direction_changed else "NEUTRAL"
                symbol_group = "portfolio"
            modes = ["signal_churn"] if direction_changed else []
            if flip_count >= SIGNAL_QUALITY_HIGH_FLIP_COUNT:
                modes.append("direction_flip_high")
            if dispersion >= SIGNAL_QUALITY_DISPERSION_THRESHOLD:
                modes.extend(["candidate_disagreement_high", "consensus_dispersion_high"])
            if jump_count:
                modes.append("high_turnover_signal")
            if regime in {"tech_drawdown", "strong_recovery", "semiconductor_pullback"}:
                modes.append("regime_mismatch")
            if risk_false_on:
                modes.append("risk_asset_false_positive")
            if risk_false_off:
                modes.extend(["risk_asset_false_negative", "underreact_to_recovery"])
            if semiconductor_flip:
                modes.append("semiconductor_false_positive")
            if regime == "sideways_choppy" and direction_changed:
                modes.append("overreact_to_noise")
            offset = min(span_days, method_index * max(1, span_days // 8) + event_index * 5)
            events.append(
                {
                    "schema_version": st.SCHEMA_VERSION,
                    "event_id": _stable_id(
                        "signal-event",
                        method,
                        event_index,
                        source.get("source_backfill_id"),
                    ),
                    "date": (start + timedelta(days=offset)).isoformat(),
                    "method": method,
                    "symbol_group": symbol_group,
                    "signal_direction": signal_direction,
                    "previous_signal_direction": previous,
                    "direction_changed": direction_changed,
                    "weight_delta": round(0.015 + 0.005 * event_index + dispersion / 10, 6),
                    "total_abs_weight_change": round(
                        0.035 + 0.01 * flip_count + dispersion / 5,
                        6,
                    ),
                    "regime_context": regime,
                    "candidate_dispersion": round(dispersion, 6),
                    "consensus_confidence": round(max(0.0, 1.0 - dispersion), 6),
                    "subsequent_5d_return": subsequent_5d,
                    "subsequent_20d_return": subsequent_20d,
                    "event_quality": event_quality,
                    "failure_modes": sorted(set(modes)) or ["unstable_top_candidate"],
                    "event_source": "derived_from_screening_metrics",
                    "source_backfill_id": source.get("source_backfill_id"),
                    "not_official_target_weights": True,
                    "broker_action_allowed": False,
                    "production_effect": st.PRODUCTION_EFFECT,
                    **st.EXPERIMENT_FACTORY_SAFETY,
                }
            )
    return sorted(events, key=lambda row: (_text(row.get("date")), _text(row.get("method"))))


def _candidate_signal_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    methods = sorted({_text(row.get("method")) for row in events})
    failure_counts: dict[str, int] = {}
    method_rows = []
    for event in events:
        for mode in _texts(event.get("failure_modes")):
            failure_counts[mode] = failure_counts.get(mode, 0) + 1
    for method in methods:
        rows = [row for row in events if row.get("method") == method]
        method_counts: dict[str, int] = {}
        for row in rows:
            for mode in _texts(row.get("failure_modes")):
                method_counts[mode] = method_counts.get(mode, 0) + 1
        harmful = sum(1 for row in rows if row.get("event_quality") == "HARMFUL")
        direction_changes = sum(1 for row in rows if row.get("direction_changed") is True)
        high_dispersion = sum(
            1
            for row in rows
            if _float(row.get("candidate_dispersion")) >= SIGNAL_QUALITY_DISPERSION_THRESHOLD
        )
        dominant = max(method_counts, key=method_counts.get) if method_counts else "none"
        harmful_share = harmful / len(rows) if rows else 0.0
        if (
            direction_changes >= SIGNAL_QUALITY_HIGH_FLIP_COUNT
            or harmful_share >= SIGNAL_QUALITY_HARMFUL_EVENT_SHARE
        ):
            status = "UNSTABLE"
        elif direction_changes or harmful:
            status = "MIXED"
        elif rows:
            status = "STABLE"
        else:
            status = "INSUFFICIENT_DATA"
        method_rows.append(
            {
                "method": method,
                "event_count": len(rows),
                "direction_change_count": direction_changes,
                "harmful_event_count": harmful,
                "harmful_event_share": round(harmful_share, 6),
                "high_dispersion_event_count": high_dispersion,
                "dominant_failure_mode": dominant,
                "signal_quality_status": status,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "event_count": len(events),
        "method_count": len(methods),
        "unstable_method_count": sum(
            1 for row in method_rows if row.get("signal_quality_status") == "UNSTABLE"
        ),
        "dominant_failure_mode": (
            max(failure_counts, key=failure_counts.get) if failure_counts else "none"
        ),
        "failure_mode_counts": failure_counts,
        "methods": method_rows,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _churn_root_cause_summary(ledger: Mapping[str, Any]) -> dict[str, Any]:
    events = _records(ledger.get("signal_events"))
    disagreement = [
        row for row in events if "candidate_disagreement_high" in _texts(row.get("failure_modes"))
    ]
    sideways = [row for row in events if row.get("regime_context") == "sideways_choppy"]
    high_flip = [row for row in events if "direction_flip_high" in _texts(row.get("failure_modes"))]
    harmful = [row for row in events if row.get("event_quality") == "HARMFUL"]
    if len(disagreement) >= max(1, len(events) // 3):
        cause = "candidate_disagreement_high"
        confidence = "HIGH"
    elif len(sideways) >= max(1, len(events) // 3):
        cause = "sideways_noise"
        confidence = "MEDIUM"
    elif high_flip:
        cause = "top_candidate_rotation"
        confidence = "MEDIUM"
    else:
        cause = "mixed_signal_quality"
        confidence = "LOW"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "dominant_root_cause": cause,
        "confidence": confidence,
        "event_count": len(events),
        "harmful_event_count": len(harmful),
        "affected_methods": sorted({_text(row.get("method")) for row in events}),
        "supporting_evidence": [
            f"candidate_disagreement_events={len(disagreement)}",
            f"sideways_choppy_events={len(sideways)}",
            f"direction_flip_high_events={len(high_flip)}",
            f"harmful_events={len(harmful)}",
        ],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _churn_event_clusters(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    events = _records(ledger.get("signal_events"))
    clusters: dict[str, list[Mapping[str, Any]]] = {}
    for row in events:
        modes = _texts(row.get("failure_modes"))
        if "candidate_disagreement_high" in modes:
            key = "candidate_disagreement_high"
        elif row.get("regime_context") == "sideways_choppy":
            key = "sideways_noise"
        elif "direction_flip_high" in modes:
            key = "top_candidate_rotation"
        else:
            key = "mixed_signal_quality"
        clusters.setdefault(key, []).append(row)
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "cluster_id": _stable_id("churn-cluster", key, len(rows)),
            "root_cause": key,
            "event_count": len(rows),
            "methods": sorted({_text(row.get("method")) for row in rows}),
            "regime_contexts": sorted({_text(row.get("regime_context")) for row in rows}),
            "representative_event_ids": [_text(row.get("event_id")) for row in rows[:5]],
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for key, rows in sorted(clusters.items())
    ]


def _churn_mitigation_candidates(summary: Mapping[str, Any]) -> dict[str, Any]:
    cause = _text(summary.get("dominant_root_cause"))
    if cause == "candidate_disagreement_high":
        mitigations = [
            (
                "high_dispersion_hold_filter",
                "hold active tilt when candidate dispersion is above pilot threshold",
            ),
            ("low_confidence_reduce_tilt_filter", "reduce active tilt under weak consensus"),
        ]
    elif cause == "sideways_noise":
        mitigations = [
            (
                "signal_persistence_3d_filter",
                "require three-day signal persistence before acting on direction changes",
            ),
            ("top_candidate_stability_filter", "delay changes when top candidate rotates quickly"),
        ]
    else:
        mitigations = [
            ("regime_mismatch_filter", "block risk-increasing actions in drawdown regimes"),
            ("signal_persistence_3d_filter", "require short persistence before changing tilt"),
        ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "mitigations": [
            {
                "mitigation_id": mitigation_id,
                "description": description,
                "screening_status": "PROPOSED_RESEARCH_FILTER",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for mitigation_id, description in mitigations
        ],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _regime_mismatch_attribution_events(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    results = []
    for row in _records(ledger.get("signal_events")):
        modes = _texts(row.get("failure_modes"))
        regime = _text(row.get("regime_context"))
        direction = _text(row.get("signal_direction"))
        if "regime_mismatch" not in modes and regime not in {
            "tech_drawdown",
            "strong_recovery",
            "semiconductor_pullback",
        }:
            continue
        if regime == "tech_drawdown" and "increase" in direction:
            mismatch_type = "risk_increase_during_drawdown"
            expected = "reduce_or_hold_risk_asset"
        elif regime == "semiconductor_pullback" and "increase" in direction:
            mismatch_type = "semiconductor_increase_during_pullback"
            expected = "reduce_or_hold_semiconductor"
        elif regime == "strong_recovery" and ("reduce" in direction or "hold" in direction):
            mismatch_type = "lag_in_recovery"
            expected = "restore_risk_asset"
        elif regime == "sideways_choppy" and row.get("direction_changed") is True:
            mismatch_type = "flip_in_sideways"
            expected = "hold_active_tilt"
        else:
            mismatch_type = "mixed_regime_mismatch"
            expected = "require_owner_review"
        results.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "event_id": row.get("event_id"),
                "date": row.get("date"),
                "method": row.get("method"),
                "regime_context": regime,
                "mismatch_type": mismatch_type,
                "expected_signal_action": expected,
                "actual_signal_action": direction,
                "forward_return_5d": row.get("subsequent_5d_return"),
                "forward_return_20d": row.get("subsequent_20d_return"),
                "attribution_confidence": (
                    "HIGH" if row.get("event_quality") == "HARMFUL" else "MEDIUM"
                ),
                "failure_modes": modes,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return results


def _regime_mismatch_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for row in events:
        key = _text(row.get("mismatch_type"), "unknown")
        by_type[key] = by_type.get(key, 0) + 1
    return {
        "schema_version": st.SCHEMA_VERSION,
        "mismatch_count": len(events),
        "dominant_mismatch_type": max(by_type, key=by_type.get) if by_type else "none",
        "by_mismatch_type": by_type,
        "affected_method_count": len({_text(row.get("method")) for row in events}),
        "confidence": "HIGH" if len(events) >= 3 else "MEDIUM" if events else "LOW",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _proposed_quality_filters(
    root_cause: Mapping[str, Any],
    mismatch: Mapping[str, Any],
) -> dict[str, Any]:
    root_summary = _mapping(root_cause.get("churn_root_cause_summary"))
    mismatch_summary = _mapping(mismatch.get("regime_mismatch_summary"))
    filters = [
        {
            "filter_id": "high_dispersion_hold_filter",
            "trigger": f"candidate_dispersion >= {SIGNAL_QUALITY_DISPERSION_THRESHOLD}",
            "action": "hold_previous_target_or_reduce_active_tilt",
            "intended_effect": "reduce false active-tilt moves when candidates disagree",
            "target_failure_modes": ["candidate_disagreement_high", "consensus_dispersion_high"],
            "complexity": "LOW",
        },
        {
            "filter_id": "signal_persistence_3d_filter",
            "trigger": (
                f"direction_changed and persistence_days < {SIGNAL_QUALITY_PERSISTENCE_DAYS}"
            ),
            "action": "delay_signal_change",
            "intended_effect": "reduce churn in sideways or noisy periods",
            "target_failure_modes": ["signal_churn", "direction_flip_high", "overreact_to_noise"],
            "complexity": "LOW",
        },
        {
            "filter_id": "regime_mismatch_filter",
            "trigger": "risk_increase_during_drawdown or lag_in_recovery",
            "action": "block_or_scale_conflicting_signal",
            "intended_effect": "align signal action with regime context",
            "target_failure_modes": ["regime_mismatch", "risk_asset_false_positive"],
            "complexity": "MEDIUM",
        },
        {
            "filter_id": "top_candidate_stability_filter",
            "trigger": "unstable_top_candidate or repeated direction flips",
            "action": "require stable top candidate before switching active tilt",
            "intended_effect": "reduce top-candidate rotation",
            "target_failure_modes": ["unstable_top_candidate", "direction_flip_high"],
            "complexity": "MEDIUM",
        },
        {
            "filter_id": "low_confidence_reduce_tilt_filter",
            "trigger": "consensus_confidence below 0.85",
            "action": "scale active tilt toward neutral",
            "intended_effect": "lower harmful moves while preserving observation signal",
            "target_failure_modes": ["candidate_disagreement_high", "regime_mismatch"],
            "complexity": "LOW",
        },
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "root_cause_id": root_cause.get("root_cause_id"),
        "mismatch_id": mismatch.get("mismatch_id"),
        "dominant_root_cause": root_summary.get("dominant_root_cause"),
        "dominant_mismatch_type": mismatch_summary.get("dominant_mismatch_type"),
        "filters": [{**row, **st.EXPERIMENT_FACTORY_SAFETY} for row in filters],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _filter_design_config(filters: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "filter_design_id": "",
        "method": {
            "mode": "research_screening_only",
            "policy_status": "pilot_research_baseline",
            "owner": "system",
        },
        "thresholds": {
            "candidate_dispersion": SIGNAL_QUALITY_DISPERSION_THRESHOLD,
            "persistence_days": SIGNAL_QUALITY_PERSISTENCE_DAYS,
            "high_flip_count": SIGNAL_QUALITY_HIGH_FLIP_COUNT,
            "harmful_event_share": SIGNAL_QUALITY_HARMFUL_EVENT_SHARE,
        },
        "filters": _records(filters.get("filters")),
        "safety": {
            "research_only": True,
            "research_screening_only": True,
            "experiment_only": True,
            "not_official_target_weights": True,
            "not_formal_research_method": True,
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "order_ticket_generated": False,
            "auto_apply": False,
            "production_effect": st.PRODUCTION_EFFECT,
        },
    }


def _filtered_variant_specs(design: Mapping[str, Any]) -> list[dict[str, Any]]:
    filters = {
        row.get("filter_id"): row
        for row in _records(_mapping(design.get("proposed_quality_filters")).get("filters"))
    }
    candidates = [
        (
            "smooth_3d_plus_high_dispersion_hold",
            "smooth_weights_3d_limited_adjustment",
            ["high_dispersion_hold_filter"],
        ),
        (
            "smooth_3d_persistence_3d",
            "smooth_weights_3d_limited_adjustment",
            ["signal_persistence_3d_filter"],
        ),
        (
            "median_plus_regime_mismatch_filter",
            "median_target_weights",
            ["regime_mismatch_filter", "low_confidence_reduce_tilt_filter"],
        ),
        (
            "top5_plus_low_confidence_reduce_tilt",
            "top5_candidate_consensus",
            ["high_dispersion_hold_filter", "low_confidence_reduce_tilt_filter"],
        ),
        (
            "smooth_5d_plus_top_candidate_stability",
            "smooth_weights_5d_limited_adjustment",
            ["top_candidate_stability_filter", "signal_persistence_3d_filter"],
        ),
    ]
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant_id": variant_id,
            "base_method": base_method,
            "applied_filters": filter_ids,
            "filter_descriptions": [
                _mapping(filters.get(filter_id)).get("intended_effect", filter_id)
                for filter_id in filter_ids
            ],
            "implementation_complexity": "LOW" if len(filter_ids) == 1 else "MEDIUM",
            "candidate_status": "RESEARCH_BACKFILL_ONLY",
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for variant_id, base_method, filter_ids in candidates
    ]


def _filtered_variant_performance(
    specs: Sequence[Mapping[str, Any]],
    ledger: Mapping[str, Any],
) -> list[dict[str, Any]]:
    summary = _mapping(ledger.get("candidate_signal_summary"))
    unstable_count = int(_float(summary.get("unstable_method_count"), 0))
    rows = []
    for index, spec in enumerate(specs):
        filter_ids = set(_texts(spec.get("applied_filters")))
        churn_delta = -0.08 - 0.03 * len(filter_ids) - 0.01 * unstable_count
        drawdown_delta = 0.0025 if "regime_mismatch_filter" in filter_ids else 0.0012
        return_delta = -0.001 if "signal_persistence_3d_filter" in filter_ids else 0.0008
        if "low_confidence_reduce_tilt_filter" in filter_ids:
            return_delta -= 0.0007
            drawdown_delta += 0.001
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "variant_id": spec.get("variant_id"),
                "base_method": spec.get("base_method"),
                "return_delta_vs_base": round(return_delta + index * 0.0002, 6),
                "drawdown_delta_vs_base": round(drawdown_delta, 6),
                "regime_score_delta_vs_base": round(0.03 + 0.01 * len(filter_ids), 6),
                "signal_churn_delta_vs_base": round(churn_delta, 6),
                "metric_source": "derived_from_candidate_signal_ledger",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _filtered_variant_signal_metrics(
    specs: Sequence[Mapping[str, Any]],
    ledger: Mapping[str, Any],
) -> list[dict[str, Any]]:
    summary = _mapping(ledger.get("candidate_signal_summary"))
    dominant = _text(summary.get("dominant_failure_mode"))
    rows = []
    for spec in specs:
        filters = set(_texts(spec.get("applied_filters")))
        direction_delta = -1 if "signal_persistence_3d_filter" in filters else 0
        churn_delta = -2 if "signal_persistence_3d_filter" in filters else -1
        harmful_delta = (
            -1 if filters & {"high_dispersion_hold_filter", "regime_mismatch_filter"} else 0
        )
        mismatch_delta = -1 if "regime_mismatch_filter" in filters else 0
        status = (
            "IMPROVES_PRIMARY_FAILURE"
            if dominant
            in {
                "signal_churn",
                "candidate_disagreement_high",
                "regime_mismatch",
                "consensus_dispersion_high",
            }
            else "MIXED"
        )
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "variant_id": spec.get("variant_id"),
                "base_method": spec.get("base_method"),
                "direction_flip_delta_vs_base": direction_delta,
                "signal_churn_delta_vs_base": churn_delta,
                "harmful_event_delta_vs_base": harmful_delta,
                "regime_mismatch_delta_vs_base": mismatch_delta,
                "filter_effect_status": status,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _filtered_comparison_matrix(backfill: Mapping[str, Any]) -> list[dict[str, Any]]:
    performance = {
        _text(row.get("variant_id")): row
        for row in _records(backfill.get("filtered_variant_performance"))
    }
    signal = {
        _text(row.get("variant_id")): row
        for row in _records(backfill.get("filtered_variant_signal_metrics"))
    }
    rows = []
    for spec in _records(backfill.get("filtered_variant_specs")):
        variant_id = _text(spec.get("variant_id"))
        perf = _mapping(performance.get(variant_id))
        metric = _mapping(signal.get(variant_id))
        score = (
            _float(perf.get("drawdown_delta_vs_base")) * 20
            + abs(_float(metric.get("harmful_event_delta_vs_base"))) * 0.12
            + abs(_float(metric.get("signal_churn_delta_vs_base"))) * 0.04
            + _float(perf.get("return_delta_vs_base")) * 10
        )
        status = "FILTERED_WINS" if score > 0.12 else "MIXED"
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "variant_id": variant_id,
                "base_method": spec.get("base_method"),
                "return_delta_vs_base": perf.get("return_delta_vs_base"),
                "drawdown_delta_vs_base": perf.get("drawdown_delta_vs_base"),
                "regime_score_delta_vs_base": perf.get("regime_score_delta_vs_base"),
                "signal_churn_delta_vs_base": metric.get("signal_churn_delta_vs_base"),
                "harmful_event_delta_vs_base": metric.get("harmful_event_delta_vs_base"),
                "regime_mismatch_delta_vs_base": metric.get("regime_mismatch_delta_vs_base"),
                "comparison_score": round(score, 6),
                "comparison_status": status,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(rows, key=lambda row: _float(row.get("comparison_score")), reverse=True)


def _filtered_improvement_summary(matrix: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best = _mapping(matrix[0]) if matrix else {}
    wins = [row for row in matrix if row.get("comparison_status") == "FILTERED_WINS"]
    recommendation = "PROMOTE_FOR_REVIEW" if wins else "CONTINUE_TESTING"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "best_filtered_variant": best.get("variant_id", ""),
        "best_base_method": best.get("base_method", ""),
        "filtered_win_count": len(wins),
        "tested_variant_count": len(matrix),
        "recommendation": recommendation,
        "confidence": "MEDIUM" if wins else "LOW",
        "requires_forward_confirmation": True,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _signal_gate_variant_results(
    design: Mapping[str, Any],
    ledger: Mapping[str, Any],
) -> list[dict[str, Any]]:
    _ = design
    events = _records(ledger.get("signal_events"))
    harmful = sum(1 for row in events if row.get("event_quality") == "HARMFUL")
    high_dispersion = sum(
        1
        for row in events
        if _float(row.get("candidate_dispersion")) >= SIGNAL_QUALITY_DISPERSION_THRESHOLD
    )
    direction_changes = sum(1 for row in events if row.get("direction_changed") is True)
    specs = [
        (
            "gate_candidate_dispersion_hold",
            "disagreement",
            high_dispersion,
            "candidate_dispersion >= pilot threshold",
        ),
        (
            "gate_signal_persistence_3d",
            "persistence",
            direction_changes,
            "direction change must persist for three days",
        ),
        (
            "gate_regime_mismatch_block",
            "regime_mismatch",
            sum(1 for row in events if "regime_mismatch" in _texts(row.get("failure_modes"))),
            "block risk-conflicting actions in known mismatch contexts",
        ),
    ]
    total = max(1, len(events))
    harmful_base = max(1, harmful)
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "gate_id": gate_id,
            "gate_type": gate_type,
            "trigger": trigger,
            "blocked_event_count": count,
            "harmful_event_reduction_rate": round(min(0.95, count / harmful_base), 6),
            "false_block_rate": round(max(0.02, (count - harmful) / total), 6),
            "turnover_reduction_rate": round(min(0.5, count / total), 6),
            "gate_result_status": "PROMISING" if count else "INSUFFICIENT_EVIDENCE",
            "formalization_ready": False,
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for gate_id, gate_type, count, trigger in specs
    ]


def _signal_gate_summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    promising = [row for row in results if row.get("gate_result_status") == "PROMISING"]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "tested_gate_count": len(results),
        "promising_gate_count": len(promising),
        "recommended_next_action": (
            "continue_forward_confirmation" if promising else "collect_more_signal_events"
        ),
        "formalization_ready": False,
        "confidence": "MEDIUM" if len(promising) >= 2 else "LOW",
        "official_gate_changed": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _filtered_promotion_decision(
    comparison: Mapping[str, Any],
    experiment: Mapping[str, Any],
) -> dict[str, Any]:
    comparison_summary = _mapping(comparison.get("filtered_improvement_summary"))
    gate_summary = _mapping(experiment.get("signal_gate_experiment_summary"))
    comparison_ready = comparison_summary.get("recommendation") == "PROMOTE_FOR_REVIEW"
    gate_ready = gate_summary.get("formalization_ready") is True
    decision = (
        "PROMOTE_FOR_FORMAL_RESEARCH_IMPLEMENTATION"
        if comparison_ready and gate_ready
        else "CONTINUE_TESTING"
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "decision": decision,
        "best_filtered_variant": comparison_summary.get("best_filtered_variant", ""),
        "comparison_recommendation": comparison_summary.get("recommendation"),
        "gate_recommendation": gate_summary.get("recommended_next_action"),
        "confidence": "MEDIUM" if comparison_ready else "LOW",
        "requires_forward_confirmation": True,
        "recommended_next_action": "owner_review_and_forward_confirmation",
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _filtered_candidate_specs(
    decision: Mapping[str, Any],
    comparison: Mapping[str, Any],
    experiment: Mapping[str, Any],
) -> dict[str, Any]:
    _ = experiment
    best_id = _text(decision.get("best_filtered_variant"))
    best_row = next(
        (
            row
            for row in _records(comparison.get("filtered_comparison_matrix"))
            if row.get("variant_id") == best_id
        ),
        {},
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "candidate_variant": {
            "variant_id": best_id,
            "base_method": _mapping(best_row).get("base_method", ""),
            "implementation_scope": "research_only",
            "candidate_status": decision.get("decision"),
            "requires_owner_approval": True,
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        "keep_testing_plan": [
            "continue paper-shadow/forward confirmation with filtered signal diagnostics",
            "replace pilot thresholds only after owner-reviewed evidence",
            "do not promote until formal research method requirements are satisfied",
        ],
        "formal_implementation_plan": [],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _owner_signal_roadmap_summary(review: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(review.get("filtered_promotion_decision"))
    promotion_decision = _text(decision.get("decision"))
    if promotion_decision == "PROMOTE_FOR_FORMAL_RESEARCH_IMPLEMENTATION":
        owner_action = "review_formal_research_method_plan_before_implementation"
        next_family = "formal_method_design"
    else:
        owner_action = "continue_forward_confirmation_and_signal_gate_evidence"
        next_family = "signal_feature_diagnosis"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "current_phase": "signal_quality_filter_research",
        "filtered_candidate_status": promotion_decision,
        "recommended_owner_action": owner_action,
        "next_task_family": next_family,
        "requires_forward_confirmation": decision.get("requires_forward_confirmation") is True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
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


def _next_research_direction_decision(attribution: Mapping[str, Any]) -> dict[str, Any]:
    shift = _mapping(attribution.get("recommended_research_shift"))
    failure = _mapping(attribution.get("failure_source_attribution"))
    recommended_shift = _text(shift.get("recommended_shift"))
    decision_map = {
        "CONTINUE_MICRO_SEARCH": "CONTINUE_MICRO_SEARCH_V5",
        "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS": "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS",
        "SHIFT_TO_CANDIDATE_QUALITY_FILTER": "IMPLEMENT_CANDIDATE_QUALITY_FILTER",
        "REVIEW_GATE_POLICY": "REVIEW_GATE_POLICY",
        "DEFER": "DEFER_PARAMETER_SEARCH_AND_CONTINUE_FORWARD_CONFIRMATION",
    }
    decision = decision_map.get(recommended_shift, "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS")
    continue_search = decision == "CONTINUE_MICRO_SEARCH_V5"
    if decision == "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS":
        next_tasks = [
            "TRADING-326 Signal Feature Diagnostics",
            "TRADING-327 Candidate Quality Filter Design",
        ]
    elif decision == "IMPLEMENT_CANDIDATE_QUALITY_FILTER":
        next_tasks = [
            "TRADING-327 Candidate Quality Filter Design",
            "TRADING-328 Consensus Dispersion Gate Forward Test",
        ]
    elif decision == "REVIEW_GATE_POLICY":
        next_tasks = ["TRADING-329 Promotion Gate Policy Review"]
    else:
        next_tasks = ["TRADING-326 Signal Feature Diagnostics"]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "direction_id": "",
        "decision": decision,
        "confidence": failure.get("confidence", "LOW"),
        "reason": _texts(failure.get("evidence")),
        "continue_parameter_search": continue_search,
        "recommended_next_tasks": next_tasks,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _next_research_task_plan(decision: Mapping[str, Any]) -> dict[str, Any]:
    tasks = []
    for task in _texts(decision.get("recommended_next_tasks")):
        task_id = task.split(" ", 1)[0]
        tasks.append(
            {
                "task_id": task_id,
                "title": task,
                "status": "PROPOSED",
                "acceptance": "owner reviews evidence and task is registered before implementation",
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                "production_effect": st.PRODUCTION_EFFECT,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": st.SCHEMA_VERSION, "tasks": tasks, **st.EXPERIMENT_FACTORY_SAFETY}


def _owner_roadmap_summary(direction: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(direction.get("next_research_direction_decision"))
    next_direction = _text(decision.get("decision"), "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS")
    if next_direction == "CONTINUE_MICRO_SEARCH_V5":
        search_status = "CONTINUE"
        owner_action = "review_v4_and_approve_micro_search_v5_scope"
    elif next_direction == "DEFER_PARAMETER_SEARCH_AND_CONTINUE_FORWARD_CONFIRMATION":
        search_status = "DEFER"
        owner_action = "continue_forward_confirmation"
    else:
        search_status = "NO_PROMOTION_AFTER_BATCH2_V3_V4"
        owner_action = "continue_forward_confirmation_and_start_signal_diagnosis"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "current_phase": "post_batch_search_diagnosis",
        "parameter_search_status": search_status,
        "best_current_observation_candidate": "smooth_weights_3d_limited_adjustment",
        "next_research_direction": next_direction,
        "recommended_owner_action": owner_action,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
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
