from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_signal_generator import (
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    round_float,
    to_float,
    write_json,
    write_markdown,
    write_matrix_artifacts,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    DEFAULT_INPUT_ROOT as DEFAULT_GENERATOR_ROOT,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_VALIDATION_ROOT,
)
from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DIAGNOSTICS_ROOT,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "candidate_generator_confidence_scaling_refinement_plan"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2287_CANDIDATE_GENERATOR_CONFIDENCE_SCALING_REFINEMENT_PLAN"
STATUS = "CONFIDENCE_SCALING_REFINEMENT_PLAN_READY_PROMOTION_BLOCKED"
MODE = "refinement_plan"
ARTIFACT_ROLE = "confidence_scaling_refinement_plan"
NEXT_TASK = "TRADING-2288_Refined_Candidate_Regeneration_with_Adjusted_Confidence_Scaling"
DEFAULT_CANDIDATES = (
    "baseline_plus_trend_structure",
    "risk_appetite",
    "volatility_regime",
)

# TRADING-2287 pilot planning constants. These are research-only refinement-design
# baselines documented in docs/requirements/TRADING-2287_*.md, not promotion gates.
HIGH_CONFIDENCE_THRESHOLD = 0.65
LOW_CONFIDENCE_THRESHOLD = 0.35
TARGET_HIGH_CONFIDENCE_RATIO_MIN = 0.10
TARGET_HIGH_CONFIDENCE_RATIO_MAX = 0.30
TARGET_LOW_CONFIDENCE_RATIO_MAX = 0.50
TARGET_NEUTRAL_RATIO_MAX = 0.60
TARGET_DIRECTIONAL_SIGNAL_RATIO_MIN = 0.25
MAX_HIGH_CONFIDENCE_RATIO_GUARDRAIL = 0.35
MIN_DIRECTIONAL_SIGNAL_RATIO_GUARDRAIL = 0.20
MAX_FALSE_SIGNAL_COST_INCREASE = 0.0
MAX_NEUTRAL_RATIO_REDUCTION = 0.30
MAX_CANDIDATE_OVERLAP_INCREASE = 0.05
MINIMUM_ELIGIBLE_RECORDS = 1000
MAX_PARAMETER_SETS_PER_CANDIDATE = 24
CONFIDENCE_CAP_RELAXATION_TRIGGER = 0.65
CONFIDENCE_STD_COMPRESSION_THRESHOLD = 0.02
NEUTRAL_BAND_FAILURE_THRESHOLD = 0.70

ALLOWED_PROPOSAL_TYPES = {
    "PIECEWISE_LINEAR_SCALING",
    "QUANTILE_BASED_SCALING",
    "DIRECTION_AWARE_SCALING",
    "HORIZON_SPECIFIC_SCALING",
    "ASSET_SPECIFIC_SCALING",
    "REGIME_AWARE_SCALING",
    "MISSING_INPUT_PENALTY_RECALIBRATION",
    "CONFIDENCE_CAP_RELAXATION",
    "NEUTRAL_BAND_NARROWING",
}
BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}
GENERATOR_FILES = {
    "baseline_plus_trend_structure": (
        "src/ai_trading_system/baseline_plus_trend_structure_generator.py"
    ),
    "risk_appetite": "src/ai_trading_system/risk_appetite_candidate_generator.py",
    "volatility_regime": "src/ai_trading_system/volatility_regime_candidate_generator.py",
}


class CandidateConfidenceScalingRefinementPlanError(ValueError):
    pass


@dataclass(frozen=True)
class CandidateGeneratorContextRow:
    candidate_id: str
    status: str
    missing_artifacts: tuple[str, ...]
    signal_names: tuple[str, ...]
    required_inputs: tuple[str, ...]
    missing_inputs: tuple[str, ...]
    proxy_input_used: bool
    proxy_limitations: tuple[str, ...]


@dataclass(frozen=True)
class CandidateConfidenceScalingInputs:
    diagnostics_dir: Path
    validation_dir: Path
    generator_dir: Path
    diagnostics_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    density_rows: list[dict[str, Any]]
    confidence_rows: list[dict[str, Any]]
    horizon_asset_rows: list[dict[str, Any]]
    direction_rows: list[dict[str, Any]]
    false_cost_rows: list[dict[str, Any]]
    overlap_rows: list[dict[str, Any]]
    data_quality_impact_rows: list[dict[str, Any]]
    recommendation_rows: list[dict[str, Any]]
    outcome_rows: list[dict[str, Any]]
    scorecard_rows: list[dict[str, Any]]
    validation_data_quality_rows: list[dict[str, Any]]
    state_recommendation_rows: list[dict[str, Any]]
    generator_context_rows: list[CandidateGeneratorContextRow]
    generator_context_status: str
    generator_context_warning_count: int


def run_candidate_generator_confidence_scaling_refinement_plan(
    *,
    diagnostics_dir: Path = DEFAULT_DIAGNOSTICS_ROOT,
    validation_dir: Path = DEFAULT_VALIDATION_ROOT,
    generator_dir: Path = DEFAULT_GENERATOR_ROOT,
    candidates: Sequence[str] | str = DEFAULT_CANDIDATES,
    target_assets: Sequence[str] | str = ("QQQ", "SPY", "SMH"),
    horizons: Sequence[str] | str = ("5d", "10d", "20d"),
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise CandidateConfidenceScalingRefinementPlanError(
            "candidate generator confidence scaling refinement plan only supports "
            "refinement_plan mode"
        )

    candidate_ids = _normalize_list(candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    inputs = load_confidence_scaling_refinement_inputs(
        diagnostics_dir=diagnostics_dir,
        validation_dir=validation_dir,
        generator_dir=generator_dir,
        candidates=candidate_ids,
    )
    outcome_rows = [
        row
        for row in inputs.outcome_rows
        if str(row.get("candidate_id")) in candidate_ids
        and str(row.get("target_asset")) in asset_ids
        and str(row.get("horizon")) in horizon_ids
    ]

    diagnosis_rows = build_candidate_confidence_failure_diagnosis_matrix(
        candidate_ids=candidate_ids,
        outcome_rows=outcome_rows,
        recommendation_rows=inputs.recommendation_rows,
        density_rows=inputs.density_rows,
        confidence_rows=inputs.confidence_rows,
        false_cost_rows=inputs.false_cost_rows,
        data_quality_impact_rows=inputs.data_quality_impact_rows,
        generator_context_rows=inputs.generator_context_rows,
    )
    retargeting_rows = build_candidate_confidence_distribution_retargeting_matrix(diagnosis_rows)
    proposal_rows = build_candidate_confidence_scaling_proposal_matrix(diagnosis_rows)
    grid_rows = build_candidate_confidence_scaling_parameter_grid(
        proposal_rows=proposal_rows,
        retargeting_rows=retargeting_rows,
    )
    guardrail_rows = build_candidate_guardrail_matrix(proposal_rows)
    risk_impact_rows = build_candidate_expected_risk_impact_matrix(
        proposal_rows=proposal_rows,
        false_cost_rows=inputs.false_cost_rows,
        overlap_rows=inputs.overlap_rows,
    )
    implementation_rows = build_candidate_2288_implementation_plan(
        candidate_ids=candidate_ids,
        diagnosis_rows=diagnosis_rows,
        proposal_rows=proposal_rows,
        grid_rows=grid_rows,
    )

    summary = _refinement_summary(
        candidate_ids=candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        generated_at=generated_at,
        inputs=inputs,
        outcome_rows=outcome_rows,
        diagnosis_rows=diagnosis_rows,
        proposal_rows=proposal_rows,
        grid_rows=grid_rows,
        matrix_count=7,
    )
    common = _common_payload(generated_at=generated_at, mode=mode, summary=summary)
    _assert_generated_payload_safe("common", common)
    for matrix_name, rows in (
        ("diagnosis", diagnosis_rows),
        ("retargeting", retargeting_rows),
        ("proposal", proposal_rows),
        ("grid", grid_rows),
        ("guardrail", guardrail_rows),
        ("risk_impact", risk_impact_rows),
        ("implementation_plan", implementation_rows),
    ):
        _assert_generated_rows_safe(matrix_name, rows)

    paths = _artifact_paths(output_dir=output_dir, docs_root=docs_root)
    write_json(paths["summary"], {**common, "summary": summary})
    write_matrix_artifacts(
        paths["failure_diagnosis_json"],
        paths["failure_diagnosis_csv"],
        common,
        diagnosis_rows,
    )
    write_matrix_artifacts(
        paths["distribution_retargeting_json"],
        paths["distribution_retargeting_csv"],
        common,
        retargeting_rows,
    )
    write_matrix_artifacts(
        paths["scaling_proposal_json"],
        paths["scaling_proposal_csv"],
        common,
        proposal_rows,
    )
    write_matrix_artifacts(
        paths["parameter_grid_json"],
        paths["parameter_grid_csv"],
        common,
        grid_rows,
    )
    write_matrix_artifacts(paths["guardrail_json"], paths["guardrail_csv"], common, guardrail_rows)
    write_matrix_artifacts(
        paths["risk_impact_json"],
        paths["risk_impact_csv"],
        common,
        risk_impact_rows,
    )
    write_matrix_artifacts(
        paths["implementation_plan_json"],
        paths["implementation_plan_csv"],
        common,
        implementation_rows,
    )

    write_markdown(
        paths["refinement_plan_doc"],
        _render_refinement_plan_doc(
            summary=summary,
            diagnosis_rows=diagnosis_rows,
            retargeting_rows=retargeting_rows,
            proposal_rows=proposal_rows,
            grid_rows=grid_rows,
        ),
    )
    write_markdown(
        paths["failure_diagnosis_doc"],
        _render_failure_diagnosis_doc(summary=summary, diagnosis_rows=diagnosis_rows),
    )
    write_markdown(
        paths["guardrails_doc"],
        _render_guardrails_doc(
            summary=summary,
            guardrail_rows=guardrail_rows,
            risk_impact_rows=risk_impact_rows,
        ),
    )
    write_markdown(
        paths["implementation_plan_doc"],
        _render_implementation_plan_doc(
            summary=summary,
            implementation_rows=implementation_rows,
            grid_rows=grid_rows,
        ),
    )

    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "candidate_confidence_failure_diagnosis": diagnosis_rows,
            "candidate_confidence_scaling_proposals": proposal_rows,
            "candidate_2288_implementation_plan": implementation_rows,
        }
    )


def load_confidence_scaling_refinement_inputs(
    *,
    diagnostics_dir: Path,
    validation_dir: Path,
    generator_dir: Path,
    candidates: Sequence[str] | str,
) -> CandidateConfidenceScalingInputs:
    candidate_ids = _normalize_list(candidates)
    diagnostics_paths = {
        "diagnostics_summary": diagnostics_dir / "inconclusive_diagnostics_summary.json",
        "signal_density": diagnostics_dir / "candidate_signal_density_matrix.json",
        "confidence_distribution": (
            diagnostics_dir / "candidate_confidence_distribution_matrix.json"
        ),
        "horizon_asset": diagnostics_dir / "candidate_horizon_asset_drilldown.json",
        "direction_alignment": (diagnostics_dir / "candidate_direction_alignment_drilldown.json"),
        "false_signal_cost": diagnostics_dir / "candidate_false_signal_cost_matrix.json",
        "signal_overlap": diagnostics_dir / "candidate_signal_overlap_matrix.json",
        "data_quality_impact": diagnostics_dir / "candidate_data_quality_impact_matrix.json",
        "refinement_recommendation": (
            diagnostics_dir / "candidate_refinement_recommendation_matrix.json"
        ),
    }
    validation_paths = {
        "validation_summary": (
            validation_dir / "regenerated_candidate_actual_path_validation_summary.json"
        ),
        "outcome_matrix": validation_dir / "candidate_prediction_outcome_matrix.json",
        "scorecard": validation_dir / "candidate_validation_scorecard.json",
        "data_quality": validation_dir / "candidate_data_quality_report.json",
        "state_recommendations": validation_dir / "candidate_state_recommendation_matrix.json",
    }
    missing = [
        str(path)
        for path in [*diagnostics_paths.values(), *validation_paths.values()]
        if not path.exists()
    ]
    if missing:
        raise CandidateConfidenceScalingRefinementPlanError(
            f"missing confidence scaling refinement input artifact(s): {missing}"
        )

    payloads = {
        **{name: _read_json(path) for name, path in diagnostics_paths.items()},
        **{name: _read_json(path) for name, path in validation_paths.items()},
    }
    for name, payload in payloads.items():
        _assert_safe_payload(name, payload, require_safety_fields=True)
        _assert_no_banned_recommendations(name, payload)
        _assert_recursive_safety(name, payload)

    recommendation_rows = _rows_from_payload(
        payloads["refinement_recommendation"],
        "rows",
    )
    scorecard_rows = _rows_from_payload(payloads["scorecard"], "candidate_scorecards")
    if not recommendation_rows:
        raise CandidateConfidenceScalingRefinementPlanError(
            "input candidate refinement recommendation matrix is empty"
        )
    if not scorecard_rows:
        raise CandidateConfidenceScalingRefinementPlanError("input actual-path scorecard is empty")

    matrix_rows = {
        "density_rows": _rows_from_payload(payloads["signal_density"], "rows"),
        "confidence_rows": _rows_from_payload(payloads["confidence_distribution"], "rows"),
        "horizon_asset_rows": _rows_from_payload(payloads["horizon_asset"], "rows"),
        "direction_rows": _rows_from_payload(payloads["direction_alignment"], "rows"),
        "false_cost_rows": _rows_from_payload(payloads["false_signal_cost"], "rows"),
        "overlap_rows": _rows_from_payload(payloads["signal_overlap"], "rows"),
        "data_quality_impact_rows": _rows_from_payload(
            payloads["data_quality_impact"],
            "rows",
        ),
        "outcome_rows": _rows_from_payload(payloads["outcome_matrix"], "rows"),
        "validation_data_quality_rows": _rows_from_payload(
            payloads["data_quality"],
            "candidate_rows",
        ),
        "state_recommendation_rows": _rows_from_payload(
            payloads["state_recommendations"],
            "candidate_rows",
        ),
    }
    for row_name, rows in matrix_rows.items():
        for index, row in enumerate(rows):
            _assert_recursive_safety(f"{row_name}[{index}]", row)
            _assert_no_banned_recommendations(f"{row_name}[{index}]", row)

    generator_context_rows = _load_generator_context_rows(generator_dir, candidate_ids)
    context_status = (
        "complete"
        if generator_context_rows
        and all(row.status == "complete" for row in generator_context_rows)
        else "partial"
    )
    warning_count = sum(1 for row in generator_context_rows if row.status != "complete")
    return CandidateConfidenceScalingInputs(
        diagnostics_dir=diagnostics_dir,
        validation_dir=validation_dir,
        generator_dir=generator_dir,
        diagnostics_summary=payloads["diagnostics_summary"],
        validation_summary=payloads["validation_summary"],
        recommendation_rows=recommendation_rows,
        scorecard_rows=scorecard_rows,
        generator_context_rows=generator_context_rows,
        generator_context_status=context_status,
        generator_context_warning_count=warning_count,
        **matrix_rows,
    )


def build_candidate_confidence_failure_diagnosis_matrix(
    *,
    candidate_ids: Sequence[str] | str,
    outcome_rows: Sequence[Mapping[str, Any]],
    recommendation_rows: Sequence[Mapping[str, Any]],
    density_rows: Sequence[Mapping[str, Any]] = (),
    confidence_rows: Sequence[Mapping[str, Any]] = (),
    false_cost_rows: Sequence[Mapping[str, Any]] = (),
    data_quality_impact_rows: Sequence[Mapping[str, Any]] = (),
    generator_context_rows: Sequence[CandidateGeneratorContextRow] = (),
) -> list[dict[str, Any]]:
    recommendation_by_candidate = {
        str(row.get("candidate_id")): dict(row) for row in recommendation_rows
    }
    context_by_candidate = {row.candidate_id: row for row in generator_context_rows}
    false_cost_by_candidate = {str(row.get("candidate_id")): dict(row) for row in false_cost_rows}
    data_quality_by_candidate = {
        str(row.get("candidate_id")): dict(row) for row in data_quality_impact_rows
    }
    density_by_candidate = _rows_by_candidate(density_rows)
    confidence_by_candidate = _rows_by_candidate(confidence_rows)
    rows: list[dict[str, Any]] = []
    for candidate_id in _normalize_list(candidate_ids):
        candidate_rows = [
            row for row in outcome_rows if str(row.get("candidate_id")) == candidate_id
        ]
        rec = recommendation_by_candidate.get(candidate_id, {})
        context = context_by_candidate.get(candidate_id)
        stats = _confidence_stats(candidate_rows)
        failure_modes = _failure_modes(
            candidate_id=candidate_id,
            stats=stats,
            recommendation=rec,
            density_rows=density_by_candidate.get(candidate_id, []),
            confidence_rows=confidence_by_candidate.get(candidate_id, []),
            false_cost_row=false_cost_by_candidate.get(candidate_id, {}),
            generator_context=context,
        )
        primary = failure_modes[0] if failure_modes else "INSUFFICIENT_HIGH_CONVICTION_RULE"
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "current_state": rec.get(
                        "current_state",
                        "ACTUAL_PATH_VALIDATED_INCONCLUSIVE",
                    ),
                    "primary_inconclusive_reason": rec.get(
                        "primary_inconclusive_reason",
                        "LOW_CONFIDENCE_SIGNAL",
                    ),
                    "recommended_action_from_2286": rec.get(
                        "recommended_next_action",
                        "REFINE_CONFIDENCE_SCALING",
                    ),
                    "record_count": stats["record_count"],
                    "eligible_record_count": stats["eligible_record_count"],
                    "neutral_ratio": round_float(stats["neutral_ratio"]),
                    "directional_signal_ratio": round_float(stats["directional_signal_ratio"]),
                    "high_confidence_ratio": round_float(stats["high_confidence_ratio"]),
                    "low_confidence_ratio": round_float(stats["low_confidence_ratio"]),
                    "median_confidence": round_float(stats["median_confidence"]),
                    "p75_confidence": round_float(stats["p75_confidence"]),
                    "p90_confidence": round_float(stats["p90_confidence"]),
                    "confidence_std": round_float(stats["confidence_std"]),
                    "max_confidence": round_float(stats["max_confidence"]),
                    "dominant_failure_mode": primary,
                    "secondary_failure_modes": failure_modes[1:],
                    "affected_assets": _affected_values(candidate_rows, "target_asset"),
                    "affected_horizons": _affected_values(candidate_rows, "horizon"),
                    "affected_directions": _affected_values(candidate_rows, "signal_direction"),
                    "data_quality_material": (
                        data_quality_by_candidate.get(candidate_id, {}).get("diagnostic_label")
                        != "DATA_QUALITY_NOT_MATERIAL"
                    ),
                    "generator_context_status": context.status if context else "partial",
                    "missing_inputs": list(context.missing_inputs) if context else [],
                    "proxy_input_used": bool(context.proxy_input_used) if context else False,
                    **_planning_boundary_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id",))


def build_candidate_confidence_distribution_retargeting_matrix(
    diagnosis_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in diagnosis_rows:
        candidate_id = str(row.get("candidate_id"))
        current_high = to_float(row.get("high_confidence_ratio"))
        current_low = to_float(row.get("low_confidence_ratio"))
        current_neutral = to_float(row.get("neutral_ratio"))
        current_directional = to_float(row.get("directional_signal_ratio"))
        target_high_max = min(
            TARGET_HIGH_CONFIDENCE_RATIO_MAX,
            MAX_HIGH_CONFIDENCE_RATIO_GUARDRAIL,
        )
        target_neutral_max = min(
            TARGET_NEUTRAL_RATIO_MAX,
            max(current_neutral - 0.05, 0.0)
            if current_neutral > TARGET_NEUTRAL_RATIO_MAX
            else TARGET_NEUTRAL_RATIO_MAX,
        )
        method = _retargeting_method(candidate_id, str(row.get("dominant_failure_mode")))
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "current_high_confidence_ratio": round_float(current_high),
                    "target_high_confidence_ratio_min": TARGET_HIGH_CONFIDENCE_RATIO_MIN,
                    "target_high_confidence_ratio_max": round_float(target_high_max),
                    "current_low_confidence_ratio": round_float(current_low),
                    "target_low_confidence_ratio_max": TARGET_LOW_CONFIDENCE_RATIO_MAX,
                    "current_neutral_ratio": round_float(current_neutral),
                    "target_neutral_ratio_max": round_float(target_neutral_max),
                    "current_directional_signal_ratio": round_float(current_directional),
                    "target_directional_signal_ratio_min": max(
                        TARGET_DIRECTIONAL_SIGNAL_RATIO_MIN,
                        MIN_DIRECTIONAL_SIGNAL_RATIO_GUARDRAIL,
                    ),
                    "confidence_retargeting_method": method,
                    "notes": (
                        "Research-only retargeting design; does not execute regeneration "
                        "or mark any candidate promotion-ready."
                    ),
                    **_planning_boundary_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id",))


def build_candidate_confidence_scaling_proposal_matrix(
    diagnosis_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for diagnosis in diagnosis_rows:
        candidate_id = str(diagnosis.get("candidate_id"))
        for template in _proposal_templates(candidate_id):
            proposal_type = template["proposal_type"]
            if proposal_type not in ALLOWED_PROPOSAL_TYPES:
                raise CandidateConfidenceScalingRefinementPlanError(
                    f"unsupported proposal type: {proposal_type}"
                )
            rows.append(
                clean_for_yaml(
                    {
                        "candidate_id": candidate_id,
                        "proposal_id": template["proposal_id"],
                        "proposal_type": proposal_type,
                        "applies_to_assets": template["assets"],
                        "applies_to_horizons": template["horizons"],
                        "applies_to_directions": template["directions"],
                        "source_failure_mode": template.get(
                            "source_failure_mode",
                            diagnosis.get("dominant_failure_mode"),
                        ),
                        "current_behavior": template["current_behavior"],
                        "proposed_change": template["proposed_change"],
                        "expected_effect": template["expected_effect"],
                        "risk_control_note": template["risk_control_note"],
                        "requires_generator_change": True,
                        "requires_validation_change": False,
                        **_planning_boundary_fields(),
                    }
                )
            )
    return _sorted_rows(rows, ("candidate_id", "proposal_id"))


def build_candidate_confidence_scaling_parameter_grid(
    *,
    proposal_rows: Sequence[Mapping[str, Any]],
    retargeting_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    retargeting_by_candidate = {str(row.get("candidate_id")): row for row in retargeting_rows}
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    variants = (
        {
            "suffix": "conservative",
            "neutral_band_width": 0.20,
            "confidence_scale_factor": 1.25,
            "confidence_cap": 0.75,
            "confidence_floor": 0.10,
            "high_confidence_threshold": 0.70,
            "low_confidence_threshold": 0.35,
            "directional_activation_threshold": 0.25,
            "missing_input_penalty": 0.25,
            "expected_high": 0.10,
            "expected_directional": 0.25,
            "guardrail_profile": "strict_cost_neutral",
        },
        {
            "suffix": "balanced",
            "neutral_band_width": 0.15,
            "confidence_scale_factor": 1.50,
            "confidence_cap": 0.85,
            "confidence_floor": 0.12,
            "high_confidence_threshold": 0.65,
            "low_confidence_threshold": 0.35,
            "directional_activation_threshold": 0.20,
            "missing_input_penalty": 0.18,
            "expected_high": 0.18,
            "expected_directional": 0.30,
            "guardrail_profile": "balanced_confidence_retarget",
        },
        {
            "suffix": "assertive",
            "neutral_band_width": 0.10,
            "confidence_scale_factor": 1.75,
            "confidence_cap": 0.95,
            "confidence_floor": 0.15,
            "high_confidence_threshold": 0.60,
            "low_confidence_threshold": 0.35,
            "directional_activation_threshold": 0.15,
            "missing_input_penalty": 0.10,
            "expected_high": 0.25,
            "expected_directional": 0.35,
            "guardrail_profile": "assertive_with_false_cost_stop",
        },
    )
    for proposal in proposal_rows:
        candidate_id = str(proposal.get("candidate_id"))
        retargeting = retargeting_by_candidate.get(candidate_id, {})
        for variant in variants:
            if counts[candidate_id] >= MAX_PARAMETER_SETS_PER_CANDIDATE:
                break
            counts[candidate_id] += 1
            proposal_id = str(proposal.get("proposal_id"))
            rows.append(
                clean_for_yaml(
                    {
                        "candidate_id": candidate_id,
                        "proposal_id": proposal_id,
                        "parameter_set_id": f"{proposal_id}_{variant['suffix']}",
                        "neutral_band_width": variant["neutral_band_width"],
                        "confidence_scale_factor": variant["confidence_scale_factor"],
                        "confidence_cap": variant["confidence_cap"],
                        "confidence_floor": variant["confidence_floor"],
                        "high_confidence_threshold": variant["high_confidence_threshold"],
                        "low_confidence_threshold": variant["low_confidence_threshold"],
                        "directional_activation_threshold": (
                            variant["directional_activation_threshold"]
                        ),
                        "missing_input_penalty": _missing_input_penalty(
                            proposal,
                            to_float(variant["missing_input_penalty"]),
                        ),
                        "asset_scope": proposal.get("applies_to_assets"),
                        "horizon_scope": proposal.get("applies_to_horizons"),
                        "regime_scope": _regime_scope(proposal),
                        "expected_high_confidence_ratio": min(
                            to_float(variant["expected_high"]),
                            to_float(
                                retargeting.get(
                                    "target_high_confidence_ratio_max",
                                    TARGET_HIGH_CONFIDENCE_RATIO_MAX,
                                )
                            ),
                            MAX_HIGH_CONFIDENCE_RATIO_GUARDRAIL,
                        ),
                        "expected_directional_signal_ratio": max(
                            to_float(variant["expected_directional"]),
                            to_float(
                                retargeting.get(
                                    "target_directional_signal_ratio_min",
                                    TARGET_DIRECTIONAL_SIGNAL_RATIO_MIN,
                                )
                            ),
                        ),
                        "guardrail_profile": variant["guardrail_profile"],
                        "regeneration_executed": False,
                        "actual_path_validation_executed": False,
                        **_planning_boundary_fields(),
                    }
                )
            )
    return _sorted_rows(rows, ("candidate_id", "proposal_id", "parameter_set_id"))


def build_candidate_guardrail_matrix(
    proposal_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for proposal in proposal_rows:
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": proposal.get("candidate_id"),
                    "proposal_id": proposal.get("proposal_id"),
                    "guardrail_profile": _guardrail_profile(proposal),
                    "max_high_confidence_ratio": MAX_HIGH_CONFIDENCE_RATIO_GUARDRAIL,
                    "min_directional_signal_ratio": MIN_DIRECTIONAL_SIGNAL_RATIO_GUARDRAIL,
                    "max_false_risk_on_cost_increase": MAX_FALSE_SIGNAL_COST_INCREASE,
                    "max_false_risk_off_cost_increase": MAX_FALSE_SIGNAL_COST_INCREASE,
                    "max_neutral_ratio_reduction": MAX_NEUTRAL_RATIO_REDUCTION,
                    "max_candidate_overlap_increase": MAX_CANDIDATE_OVERLAP_INCREASE,
                    "minimum_eligible_records": MINIMUM_ELIGIBLE_RECORDS,
                    "data_quality_required_status": ["PASS", "PASS_WITH_WARNINGS"],
                    **_planning_boundary_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id", "proposal_id"))


def build_candidate_expected_risk_impact_matrix(
    *,
    proposal_rows: Sequence[Mapping[str, Any]],
    false_cost_rows: Sequence[Mapping[str, Any]],
    overlap_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    false_cost_by_candidate = {str(row.get("candidate_id")): dict(row) for row in false_cost_rows}
    overlap_label_by_candidate = _overlap_labels_by_candidate(overlap_rows)
    rows: list[dict[str, Any]] = []
    for proposal in proposal_rows:
        candidate_id = str(proposal.get("candidate_id"))
        false_cost = false_cost_by_candidate.get(candidate_id, {})
        dominant_false = str(false_cost.get("diagnostic_label") or "FALSE_SIGNAL_COST_NOT_DOMINANT")
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "proposal_id": proposal.get("proposal_id"),
                    "expected_benefit": _expected_benefit(proposal),
                    "expected_risk": _expected_risk(proposal, dominant_false),
                    "risk_on_failure_mode": _risk_on_failure_mode(dominant_false),
                    "risk_off_failure_mode": _risk_off_failure_mode(dominant_false),
                    "possible_overfitting_risk": "medium",
                    "possible_turnover_risk": "low",
                    "possible_signal_overlap_risk": overlap_label_by_candidate.get(
                        candidate_id,
                        "unknown_overlap_requires_2288_check",
                    ),
                    "recommended_validation_focus": _validation_focus(
                        proposal,
                        dominant_false,
                    ),
                    **_planning_boundary_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id", "proposal_id"))


def build_candidate_2288_implementation_plan(
    *,
    candidate_ids: Sequence[str] | str,
    diagnosis_rows: Sequence[Mapping[str, Any]],
    proposal_rows: Sequence[Mapping[str, Any]],
    grid_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    diagnosis_by_candidate = {str(row.get("candidate_id")): dict(row) for row in diagnosis_rows}
    proposals_by_candidate = _rows_by_candidate(proposal_rows)
    grid_by_candidate = _rows_by_candidate(grid_rows)
    rows: list[dict[str, Any]] = []
    for candidate_id in _normalize_list(candidate_ids):
        selected_proposals = [
            str(row.get("proposal_id")) for row in proposals_by_candidate[candidate_id]
        ]
        selected_parameter_sets = [
            str(row.get("parameter_set_id"))
            for row in grid_by_candidate[candidate_id]
            if str(row.get("parameter_set_id", "")).endswith("_balanced")
        ]
        diagnosis = diagnosis_by_candidate.get(candidate_id, {})
        scope = _implementation_scope(diagnosis)
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "selected_proposal_ids": selected_proposals,
                    "selected_parameter_set_ids": selected_parameter_sets,
                    "implementation_scope": scope,
                    "generator_files_to_modify": [GENERATOR_FILES.get(candidate_id, "")],
                    "expected_runtime_outputs": [
                        "candidate_signal_spec.json",
                        "candidate_signal_series.csv",
                        "candidate_prediction_artifact.json",
                        "generation_summary.json",
                        "validation_summary.json",
                    ],
                    "required_tests": [
                        f"tests/research_trends/test_{candidate_id}_generator.py",
                        "tests/research_trends/test_regenerated_candidate_artifact_validation.py",
                        "tests/research_trends/test_first_layer_candidate_generators_regenerate_cli.py",
                    ],
                    "required_validation": [
                        "regenerate refined candidate artifacts",
                        "rerun candidate-bound validator",
                        "rerun regenerated candidate actual-path validation",
                        "rerun inconclusive diagnostics after 2288",
                    ],
                    "next_task": NEXT_TASK,
                    **_planning_boundary_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id",))


def _load_generator_context_rows(
    generator_dir: Path,
    candidate_ids: Sequence[str],
) -> list[CandidateGeneratorContextRow]:
    rows: list[CandidateGeneratorContextRow] = []
    for candidate_id in candidate_ids:
        candidate_dir = generator_dir / candidate_id
        paths = {
            "signal_spec": candidate_dir / "candidate_signal_spec.json",
            "signal_series": candidate_dir / "candidate_signal_series.csv",
            "prediction_artifact": candidate_dir / "candidate_prediction_artifact.json",
            "generation_summary": candidate_dir / "generation_summary.json",
            "validation_summary": candidate_dir / "validation_summary.json",
        }
        missing = tuple(key for key, path in paths.items() if not path.exists())
        status = "complete" if not missing else "partial"
        signal_names: tuple[str, ...] = ()
        required_inputs: tuple[str, ...] = ()
        missing_inputs: tuple[str, ...] = ()
        proxy_input_used = False
        proxy_limitations: tuple[str, ...] = ()
        if paths["signal_spec"].exists():
            spec = _read_json(paths["signal_spec"])
            _assert_recursive_safety(f"{candidate_id}.signal_spec", spec)
            signal_names = tuple(str(item) for item in spec.get("output_signal_names", []))
            required_inputs = tuple(str(item) for item in spec.get("required_inputs", []))
        if paths["generation_summary"].exists():
            summary = _read_json(paths["generation_summary"])
            _assert_recursive_safety(f"{candidate_id}.generation_summary", summary)
            summary_block = summary.get("summary", {})
            if isinstance(summary_block, Mapping):
                missing_inputs = tuple(
                    str(item) for item in summary_block.get("missing_inputs", [])
                )
                proxy_input_used = bool(summary_block.get("proxy_input_used"))
            if isinstance(summary.get("signal_spec"), Mapping) and not signal_names:
                nested_spec = summary["signal_spec"]
                signal_names = tuple(
                    str(item) for item in nested_spec.get("output_signal_names", [])
                )
        if paths["prediction_artifact"].exists():
            artifact = _read_json(paths["prediction_artifact"])
            _assert_recursive_safety(f"{candidate_id}.prediction_artifact", artifact)
            provenance = artifact.get("provenance", {})
            if isinstance(provenance, Mapping):
                limitations = provenance.get("proxy_limitations", [])
                if isinstance(limitations, list):
                    proxy_limitations = tuple(str(item) for item in limitations)
        if paths["validation_summary"].exists():
            _assert_recursive_safety(
                f"{candidate_id}.validation_summary",
                _read_json(paths["validation_summary"]),
            )
        rows.append(
            CandidateGeneratorContextRow(
                candidate_id=candidate_id,
                status=status,
                missing_artifacts=missing,
                signal_names=signal_names,
                required_inputs=required_inputs,
                missing_inputs=missing_inputs,
                proxy_input_used=proxy_input_used,
                proxy_limitations=proxy_limitations,
            )
        )
    return rows


def _refinement_summary(
    *,
    candidate_ids: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
    inputs: CandidateConfidenceScalingInputs,
    outcome_rows: Sequence[Mapping[str, Any]],
    diagnosis_rows: Sequence[Mapping[str, Any]],
    proposal_rows: Sequence[Mapping[str, Any]],
    grid_rows: Sequence[Mapping[str, Any]],
    matrix_count: int,
) -> dict[str, Any]:
    eligible_count = sum(1 for row in outcome_rows if bool(row.get("validation_eligible")))
    failure_counts = Counter(str(row.get("dominant_failure_mode")) for row in diagnosis_rows)
    context_partial = inputs.generator_context_status != "complete"
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "diagnostics_dir": str(inputs.diagnostics_dir),
            "validation_dir": str(inputs.validation_dir),
            "generator_dir": str(inputs.generator_dir),
            "candidate_count": len(candidate_ids),
            "candidate_ids": list(candidate_ids),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "input_prediction_outcome_record_count": len(outcome_rows),
            "input_eligible_record_count": eligible_count,
            "diagnostic_matrix_count": matrix_count,
            "proposal_count": len(proposal_rows),
            "parameter_set_count": len(grid_rows),
            "dominant_failure_mode_counts": dict(failure_counts),
            "generator_context_status": inputs.generator_context_status,
            "generator_context_warning_count": inputs.generator_context_warning_count,
            "confidence_scaling_plan_status": ("PARTIAL_CONTEXT" if context_partial else "READY"),
            "data_quality_warning": context_partial,
            "source_diagnostics_status": _summary_status(inputs.diagnostics_summary),
            "source_validation_status": _summary_status(inputs.validation_summary),
            "artifact_role": ARTIFACT_ROLE,
            "next_task": NEXT_TASK,
            "regeneration_executed": False,
            "actual_path_validation_executed": False,
            **_planning_boundary_fields(),
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    mode: str,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": "candidate_generator_confidence_scaling_refinement_plan.v1",
            "report_type": "candidate_generator_confidence_scaling_refinement_plan",
            "artifact_role": ARTIFACT_ROLE,
            "title": "Candidate Generator Confidence Scaling Refinement Plan",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": mode,
            "research_only": True,
            "summary_status": summary.get("status"),
            "regeneration_executed": False,
            "actual_path_validation_executed": False,
            **_planning_boundary_fields(),
        }
    )


def _artifact_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "confidence_scaling_refinement_summary.json",
        "failure_diagnosis_json": (
            output_dir / "candidate_confidence_failure_diagnosis_matrix.json"
        ),
        "failure_diagnosis_csv": (output_dir / "candidate_confidence_failure_diagnosis_matrix.csv"),
        "distribution_retargeting_json": (
            output_dir / "candidate_confidence_distribution_retargeting_matrix.json"
        ),
        "distribution_retargeting_csv": (
            output_dir / "candidate_confidence_distribution_retargeting_matrix.csv"
        ),
        "scaling_proposal_json": (output_dir / "candidate_confidence_scaling_proposal_matrix.json"),
        "scaling_proposal_csv": output_dir / "candidate_confidence_scaling_proposal_matrix.csv",
        "parameter_grid_json": output_dir / "candidate_confidence_scaling_parameter_grid.json",
        "parameter_grid_csv": output_dir / "candidate_confidence_scaling_parameter_grid.csv",
        "guardrail_json": output_dir / "candidate_guardrail_matrix.json",
        "guardrail_csv": output_dir / "candidate_guardrail_matrix.csv",
        "risk_impact_json": output_dir / "candidate_expected_risk_impact_matrix.json",
        "risk_impact_csv": output_dir / "candidate_expected_risk_impact_matrix.csv",
        "implementation_plan_json": output_dir / "candidate_2288_implementation_plan.json",
        "implementation_plan_csv": output_dir / "candidate_2288_implementation_plan.csv",
        "refinement_plan_doc": docs_root / "candidate_confidence_scaling_refinement_plan.md",
        "failure_diagnosis_doc": docs_root / "candidate_confidence_failure_diagnosis.md",
        "guardrails_doc": docs_root / "candidate_confidence_scaling_guardrails.md",
        "implementation_plan_doc": docs_root / "candidate_2288_refined_regeneration_plan.md",
    }


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise CandidateConfidenceScalingRefinementPlanError(f"{path}: expected JSON object")
    return payload


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    rows = payload.get(key)
    if not isinstance(rows, list):
        raise CandidateConfidenceScalingRefinementPlanError(f"payload missing list field: {key}")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _assert_safe_payload(
    scope: str,
    payload: Mapping[str, Any],
    *,
    require_safety_fields: bool,
) -> None:
    expected = {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    errors: list[str] = []
    for field, expected_value in expected.items():
        if field not in payload:
            if require_safety_fields:
                errors.append(f"{scope}: missing {field}")
            continue
        value = payload.get(field)
        if field == "broker_action":
            if str(value) != "none":
                errors.append(f"{scope}: broker_action=none required")
        elif _bool(value) is not expected_value:
            errors.append(f"{scope}: {field} must be false")
    if errors:
        raise CandidateConfidenceScalingRefinementPlanError("; ".join(errors))


def _assert_recursive_safety(scope: str, value: Any) -> None:
    if isinstance(value, Mapping):
        _assert_safe_payload(scope, value, require_safety_fields=False)
        _assert_no_banned_recommendations(scope, value)
        for key, item in value.items():
            _assert_recursive_safety(f"{scope}.{key}", item)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_recursive_safety(f"{scope}[{index}]", item)


def _assert_no_banned_recommendations(scope: str, payload: Mapping[str, Any]) -> None:
    for field, value in payload.items():
        if isinstance(value, str) and value in BANNED_RECOMMENDATIONS:
            raise CandidateConfidenceScalingRefinementPlanError(
                f"{scope}: banned recommendation emitted in {field}: {value}"
            )


def _assert_generated_payload_safe(scope: str, payload: Mapping[str, Any]) -> None:
    _assert_safe_payload(scope, payload, require_safety_fields=True)
    _assert_recursive_safety(scope, payload)


def _assert_generated_rows_safe(scope: str, rows: Sequence[Mapping[str, Any]]) -> None:
    for index, row in enumerate(rows):
        _assert_safe_payload(f"{scope}[{index}]", row, require_safety_fields=True)
        _assert_recursive_safety(f"{scope}[{index}]", row)
        if _bool(row.get("owner_review_required")):
            raise CandidateConfidenceScalingRefinementPlanError(
                f"{scope}[{index}]: owner_review_required must remain false"
            )


def _confidence_stats(candidate_rows: Sequence[Mapping[str, Any]]) -> dict[str, float | int]:
    values = [max(0.0, min(1.0, to_float(row.get("signal_confidence")))) for row in candidate_rows]
    record_count = len(candidate_rows)
    eligible_count = sum(1 for row in candidate_rows if bool(row.get("validation_eligible")))
    neutral_count = sum(
        1 for row in candidate_rows if str(row.get("signal_direction") or "neutral") == "neutral"
    )
    high_count = sum(1 for value in values if value >= HIGH_CONFIDENCE_THRESHOLD)
    low_count = sum(1 for value in values if value <= LOW_CONFIDENCE_THRESHOLD)
    directional_count = record_count - neutral_count
    return {
        "record_count": record_count,
        "eligible_record_count": eligible_count,
        "neutral_ratio": _ratio(neutral_count, record_count),
        "directional_signal_ratio": _ratio(directional_count, record_count),
        "high_confidence_ratio": _ratio(high_count, record_count),
        "low_confidence_ratio": _ratio(low_count, record_count),
        "median_confidence": _percentile(values, 0.50),
        "p75_confidence": _percentile(values, 0.75),
        "p90_confidence": _percentile(values, 0.90),
        "confidence_std": _std(values),
        "max_confidence": max(values) if values else 0.0,
    }


def _failure_modes(
    *,
    candidate_id: str,
    stats: Mapping[str, float | int],
    recommendation: Mapping[str, Any],
    density_rows: Sequence[Mapping[str, Any]],
    confidence_rows: Sequence[Mapping[str, Any]],
    false_cost_row: Mapping[str, Any],
    generator_context: CandidateGeneratorContextRow | None,
) -> list[str]:
    modes: list[str] = []
    secondary_2286 = [
        str(item) for item in recommendation.get("secondary_inconclusive_reasons", [])
    ]
    max_confidence = to_float(stats.get("max_confidence"))
    if to_float(stats.get("high_confidence_ratio")) < TARGET_HIGH_CONFIDENCE_RATIO_MIN:
        modes.append("INSUFFICIENT_HIGH_CONVICTION_RULE")
    if to_float(stats.get("neutral_ratio")) > NEUTRAL_BAND_FAILURE_THRESHOLD:
        modes.append("NEUTRAL_BAND_TOO_WIDE")
    if (
        max_confidence < CONFIDENCE_CAP_RELAXATION_TRIGGER
        or to_float(stats.get("p90_confidence")) < CONFIDENCE_CAP_RELAXATION_TRIGGER
    ):
        modes.append("OVER_CONSERVATIVE_CONFIDENCE_CAP")
    if to_float(stats.get("directional_signal_ratio")) < TARGET_DIRECTIONAL_SIGNAL_RATIO_MIN:
        modes.append("DIRECTIONAL_SCORE_TOO_WEAK")
    if to_float(stats.get("confidence_std")) < CONFIDENCE_STD_COMPRESSION_THRESHOLD:
        modes.append("COMPONENT_SCORE_COMPRESSION")
    if "HORIZON_MISMATCH" in secondary_2286:
        modes.append("HORIZON_AVERAGING_DILUTES_SIGNAL")
    if "ASSET_SPECIFIC_MIXED_RESULTS" in secondary_2286:
        modes.append("CROSS_ASSET_AVERAGING_DILUTES_SIGNAL")
    if "REGIME_SPECIFIC_ONLY" in secondary_2286:
        modes.append("REGIME_MIXING_DILUTES_SIGNAL")
    if (
        generator_context
        and generator_context.proxy_input_used
        and generator_context.missing_inputs
    ):
        modes.append("OVER_PENALIZED_MISSING_PROXY_INPUT")
    if str(false_cost_row.get("diagnostic_label")) == "FALSE_RISK_ON_COST_TOO_HIGH":
        modes.append("DIRECTIONAL_SCORE_TOO_WEAK")
    if candidate_id == "volatility_regime":
        modes.extend(["VOLATILITY_DAMPENING_TOO_STRONG", "OVER_CONSERVATIVE_CONFIDENCE_CAP"])
    if candidate_id == "risk_appetite" and generator_context and generator_context.missing_inputs:
        modes.append("OVER_PENALIZED_MISSING_PROXY_INPUT")
    if candidate_id == "baseline_plus_trend_structure":
        modes.extend(["COMPONENT_SCORE_COMPRESSION", "NEUTRAL_BAND_TOO_WIDE"])
    if any(row.get("diagnostic_label") == "LOW_CONVICTION_SIGNAL_DESIGN" for row in density_rows):
        modes.append("INSUFFICIENT_HIGH_CONVICTION_RULE")
    if any(
        to_float(row.get("max_confidence")) < HIGH_CONFIDENCE_THRESHOLD for row in confidence_rows
    ):
        modes.append("OVER_CONSERVATIVE_CONFIDENCE_CAP")
    return _unique(modes)


def _proposal_templates(candidate_id: str) -> list[dict[str, Any]]:
    common_assets = ["QQQ", "SPY", "SMH"]
    common_horizons = ["5d", "10d", "20d"]
    if candidate_id == "baseline_plus_trend_structure":
        return [
            {
                "proposal_id": "baseline_trend_neutral_band_narrowing",
                "proposal_type": "NEUTRAL_BAND_NARROWING",
                "assets": common_assets,
                "horizons": common_horizons,
                "directions": ["trend_confirming", "trend_weakening", "neutral"],
                "source_failure_mode": "NEUTRAL_BAND_TOO_WIDE",
                "current_behavior": (
                    "Trend structure confidence is capped below high-confidence threshold."
                ),
                "proposed_change": (
                    "Narrow the trend structure neutral band before confidence scaling."
                ),
                "expected_effect": (
                    "Increase directional evidence density without changing signal source."
                ),
                "risk_control_note": (
                    "Reject in 2288 if false risk-on or false risk-off cost rises."
                ),
            },
            {
                "proposal_id": "baseline_trend_direction_aware_scaling",
                "proposal_type": "DIRECTION_AWARE_SCALING",
                "assets": common_assets,
                "horizons": common_horizons,
                "directions": ["trend_confirming", "trend_weakening"],
                "source_failure_mode": "INSUFFICIENT_HIGH_CONVICTION_RULE",
                "current_behavior": "Trend-confirming and weakening states share flat confidence.",
                "proposed_change": (
                    "Scale confidence by distance from directional activation threshold."
                ),
                "expected_effect": "Separate low, medium, and high conviction trend states.",
                "risk_control_note": (
                    "Do not elevate neutral records into high-confidence directional records."
                ),
            },
            {
                "proposal_id": "baseline_trend_horizon_specific_scaling",
                "proposal_type": "HORIZON_SPECIFIC_SCALING",
                "assets": common_assets,
                "horizons": common_horizons,
                "directions": ["risk_on", "risk_off", "trend_confirming", "trend_weakening"],
                "source_failure_mode": "HORIZON_AVERAGING_DILUTES_SIGNAL",
                "current_behavior": "5d, 10d, and 20d use the same confidence surface.",
                "proposed_change": (
                    "Use horizon-specific scaling parameters with the same source features."
                ),
                "expected_effect": (
                    "Reduce horizon averaging dilution while preserving artifact contract."
                ),
                "risk_control_note": "Require horizon-specific actual-path validation in 2288.",
            },
        ]
    if candidate_id == "risk_appetite":
        return [
            {
                "proposal_id": "risk_appetite_missing_proxy_penalty_recalibration",
                "proposal_type": "MISSING_INPUT_PENALTY_RECALIBRATION",
                "assets": common_assets,
                "horizons": common_horizons,
                "directions": ["risk_on", "risk_off", "neutral"],
                "source_failure_mode": "OVER_PENALIZED_MISSING_PROXY_INPUT",
                "current_behavior": (
                    "Missing optional proxy inputs suppress confidence when core proxies agree."
                ),
                "proposed_change": (
                    "Reduce missing-input penalty only when QQQ/SPY/SMH core proxies agree."
                ),
                "expected_effect": (
                    "Recover high-conviction risk appetite records without fabricating proxies."
                ),
                "risk_control_note": (
                    "Keep missing input disclosure and reject if false risk-on cost rises."
                ),
            },
            {
                "proposal_id": "risk_appetite_asset_specific_scaling",
                "proposal_type": "ASSET_SPECIFIC_SCALING",
                "assets": ["SMH", "QQQ", "SPY"],
                "horizons": common_horizons,
                "directions": ["risk_on", "risk_off"],
                "source_failure_mode": "CROSS_ASSET_AVERAGING_DILUTES_SIGNAL",
                "current_behavior": (
                    "Semiconductor risk appetite is diluted by broad-market averaging."
                ),
                "proposed_change": "Add asset-specific confidence scaling for SMH vs QQQ/SPY.",
                "expected_effect": (
                    "Expose asset-local risk appetite without changing candidate family."
                ),
                "risk_control_note": "Require asset-specific false signal cost checks in 2288.",
            },
            {
                "proposal_id": "risk_appetite_asymmetric_direction_scaling",
                "proposal_type": "DIRECTION_AWARE_SCALING",
                "assets": common_assets,
                "horizons": common_horizons,
                "directions": ["risk_on", "risk_off"],
                "source_failure_mode": "INSUFFICIENT_HIGH_CONVICTION_RULE",
                "current_behavior": (
                    "Risk-on confirmation and risk-off pressure use symmetric confidence."
                ),
                "proposed_change": "Scale risk-off pressure and risk-on confirmation separately.",
                "expected_effect": (
                    "Improve high-conviction risk gating without declaring a trade trigger."
                ),
                "risk_control_note": (
                    "Risk appetite remains confirm/limiter input, not standalone rebalance signal."
                ),
            },
        ]
    return [
        {
            "proposal_id": "volatility_regime_confidence_cap_relaxation",
            "proposal_type": "CONFIDENCE_CAP_RELAXATION",
            "assets": common_assets,
            "horizons": common_horizons,
            "directions": ["volatility_expansion", "risk_off", "volatility_compression"],
            "source_failure_mode": "OVER_CONSERVATIVE_CONFIDENCE_CAP",
            "current_behavior": (
                "Volatility regime confidence remains capped below high-confidence threshold."
            ),
            "proposed_change": (
                "Relax cap only when expansion/stress evidence is internally consistent."
            ),
            "expected_effect": "Create validated high-conviction volatility expansion candidates.",
            "risk_control_note": (
                "Cap relaxation must not increase false risk-off missed-upside cost."
            ),
        },
        {
            "proposal_id": "volatility_regime_percentile_scaling",
            "proposal_type": "QUANTILE_BASED_SCALING",
            "assets": common_assets,
            "horizons": common_horizons,
            "directions": ["volatility_expansion", "volatility_compression"],
            "source_failure_mode": "VOLATILITY_DAMPENING_TOO_STRONG",
            "current_behavior": (
                "Realized volatility proxy is smoothed before confidence assignment."
            ),
            "proposed_change": "Scale confidence by realized-volatility percentile distance.",
            "expected_effect": (
                "Differentiate compression, expansion, and stress records more clearly."
            ),
            "risk_control_note": "Percentile policy must be PIT-safe and asset-local in 2288.",
        },
        {
            "proposal_id": "volatility_regime_stress_override_guarded",
            "proposal_type": "REGIME_AWARE_SCALING",
            "assets": common_assets,
            "horizons": common_horizons,
            "directions": ["volatility_expansion", "risk_off"],
            "source_failure_mode": "DIRECTIONAL_SCORE_TOO_WEAK",
            "current_behavior": (
                "Stress regime signal does not form high-conviction risk-off evidence."
            ),
            "proposed_change": "Allow guarded stress-regime high-conviction override.",
            "expected_effect": "Improve risk-control utility in expansion/stress regimes.",
            "risk_control_note": "Override is blocked if it raises false risk-off cost or overlap.",
        },
    ]


def _render_refinement_plan_doc(
    *,
    summary: Mapping[str, Any],
    diagnosis_rows: Sequence[Mapping[str, Any]],
    retargeting_rows: Sequence[Mapping[str, Any]],
    proposal_rows: Sequence[Mapping[str, Any]],
    grid_rows: Sequence[Mapping[str, Any]],
) -> str:
    input_record_count = summary.get("input_prediction_outcome_record_count")
    lines = [
        "# Candidate Confidence Scaling Refinement Plan",
        "",
        f"- status: `{summary.get('status')}`",
        f"- task_id: `{TASK_ID}`",
        f"- candidate_count: `{summary.get('candidate_count')}`",
        f"- input_prediction_outcome_record_count: `{input_record_count}`",
        f"- input_eligible_record_count: `{summary.get('input_eligible_record_count')}`",
        f"- proposal_count: `{summary.get('proposal_count')}`",
        f"- parameter_set_count: `{summary.get('parameter_set_count')}`",
        f"- next_task: `{NEXT_TASK}`",
        "",
        (
            "TRADING-2287 只生成 confidence scaling refinement plan，不执行 regeneration、"
            "不重跑 actual-path validation、不做 promotion、paper-shadow、production "
            "或 broker action。"
        ),
        "",
        "|candidate_id|dominant_failure_mode|high_confidence_ratio|proposals|retarget_method|",
        "|---|---|---:|---:|---|",
    ]
    proposals_by_candidate = _rows_by_candidate(proposal_rows)
    retarget_by_candidate = {str(row.get("candidate_id")): row for row in retargeting_rows}
    for row in diagnosis_rows:
        candidate_id = str(row.get("candidate_id"))
        lines.append(
            "|{candidate}|{failure}|{high}|{proposal_count}|{method}|".format(
                candidate=candidate_id,
                failure=row.get("dominant_failure_mode"),
                high=row.get("high_confidence_ratio"),
                proposal_count=len(proposals_by_candidate.get(candidate_id, [])),
                method=retarget_by_candidate.get(candidate_id, {}).get(
                    "confidence_retargeting_method",
                    "",
                ),
            )
        )
    lines.extend(
        [
            "",
            (
                f"Parameter grid 上限为每个 candidate `{MAX_PARAMETER_SETS_PER_CANDIDATE}` "
                f"组；本次实际生成 `{len(grid_rows)}` 组。所有参数仅是 "
                "TRADING-2288 refinement design，不是验证通过或上线资格。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_failure_diagnosis_doc(
    *,
    summary: Mapping[str, Any],
    diagnosis_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Candidate Confidence Failure Diagnosis",
        "",
        f"- source_diagnostics_status: `{summary.get('source_diagnostics_status')}`",
        "- TRADING-2286 primary reason: `LOW_CONFIDENCE_SIGNAL`",
        "- TRADING-2287 不改变 TRADING-2285 的 inconclusive conclusion。",
        "",
        "|candidate_id|dominant_failure_mode|secondary_failure_modes|median_confidence|p90_confidence|neutral_ratio|directional_signal_ratio|data_quality_material|",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in diagnosis_rows:
        lines.append(
            "|{candidate}|{dominant}|{secondary}|{median}|{p90}|{neutral}|{directional}|{dq}|".format(
                candidate=row.get("candidate_id"),
                dominant=row.get("dominant_failure_mode"),
                secondary=", ".join(str(item) for item in row.get("secondary_failure_modes", [])),
                median=row.get("median_confidence"),
                p90=row.get("p90_confidence"),
                neutral=row.get("neutral_ratio"),
                directional=row.get("directional_signal_ratio"),
                dq=row.get("data_quality_material"),
            )
        )
    return "\n".join(lines) + "\n"


def _render_guardrails_doc(
    *,
    summary: Mapping[str, Any],
    guardrail_rows: Sequence[Mapping[str, Any]],
    risk_impact_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Candidate Confidence Scaling Guardrails",
        "",
        f"- status: `{summary.get('status')}`",
        f"- max_high_confidence_ratio: `{MAX_HIGH_CONFIDENCE_RATIO_GUARDRAIL}`",
        f"- max_false_risk_on_cost_increase: `{MAX_FALSE_SIGNAL_COST_INCREASE}`",
        f"- max_false_risk_off_cost_increase: `{MAX_FALSE_SIGNAL_COST_INCREASE}`",
        "",
        (
            "Guardrails 是 TRADING-2288 validation 约束，不是当前任务的通过标准，"
            "也不是 promotion gate。"
        ),
        "",
        "|candidate_id|proposal_id|guardrail_profile|recommended_validation_focus|expected_risk|",
        "|---|---|---|---|---|",
    ]
    risk_by_proposal = {
        (str(row.get("candidate_id")), str(row.get("proposal_id"))): row for row in risk_impact_rows
    }
    for row in guardrail_rows:
        key = (str(row.get("candidate_id")), str(row.get("proposal_id")))
        risk = risk_by_proposal.get(key, {})
        lines.append(
            "|{candidate}|{proposal}|{profile}|{focus}|{risk}|".format(
                candidate=row.get("candidate_id"),
                proposal=row.get("proposal_id"),
                profile=row.get("guardrail_profile"),
                focus=", ".join(str(item) for item in risk.get("recommended_validation_focus", [])),
                risk=risk.get("expected_risk", ""),
            )
        )
    return "\n".join(lines) + "\n"


def _render_implementation_plan_doc(
    *,
    summary: Mapping[str, Any],
    implementation_rows: Sequence[Mapping[str, Any]],
    grid_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Candidate 2288 Refined Regeneration Plan",
        "",
        f"- next_task: `{summary.get('next_task')}`",
        "- recommended default scope: `CONFIDENCE_SCALING_ONLY`",
        "- regeneration_executed: `false`",
        "- actual_path_validation_executed: `false`",
        "",
        "|candidate_id|implementation_scope|selected_proposal_count|selected_parameter_set_count|generator_files_to_modify|",
        "|---|---|---:|---:|---|",
    ]
    grid_by_candidate = _rows_by_candidate(grid_rows)
    for row in implementation_rows:
        candidate_id = str(row.get("candidate_id"))
        lines.append(
            "|{candidate}|{scope}|{proposal_count}|{grid_count}|{files}|".format(
                candidate=candidate_id,
                scope=row.get("implementation_scope"),
                proposal_count=len(row.get("selected_proposal_ids", [])),
                grid_count=len(grid_by_candidate.get(candidate_id, [])),
                files=", ".join(str(item) for item in row.get("generator_files_to_modify", [])),
            )
        )
    lines.extend(
        [
            "",
            (
                "TRADING-2288 才允许实现 adjusted confidence scaling 并重新生成 "
                "candidate artifacts；2287 不修改 2284 artifacts。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    return tuple(str(item).strip() for item in value if str(item).strip())


def _planning_boundary_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "owner_review_required": False,
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
        "trading_2285_inconclusive_decisions_changed": False,
        **trading_2281_boundary_fields(),
    }


def _summary_status(payload: Mapping[str, Any]) -> str:
    summary = payload.get("summary")
    if isinstance(summary, Mapping) and summary.get("status"):
        return str(summary["status"])
    return str(payload.get("status") or payload.get("summary_status") or "UNKNOWN")


def _rows_by_candidate(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("candidate_id") or "")].append(dict(row))
    return grouped


def _sorted_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows],
        key=lambda row: tuple(str(row.get(key) or "") for key in keys),
    )


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _percentile(values: Sequence[float], quantile: float) -> float:
    clean_values = sorted(value for value in values if math.isfinite(value))
    if not clean_values:
        return 0.0
    if len(clean_values) == 1:
        return clean_values[0]
    position = (len(clean_values) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean_values[lower]
    weight = position - lower
    return clean_values[lower] * (1 - weight) + clean_values[upper] * weight


def _std(values: Sequence[float]) -> float:
    clean_values = [value for value in values if math.isfinite(value)]
    if len(clean_values) < 2:
        return 0.0
    mean = sum(clean_values) / len(clean_values)
    variance = sum((value - mean) ** 2 for value in clean_values) / len(clean_values)
    return math.sqrt(variance)


def _affected_values(rows: Sequence[Mapping[str, Any]], field: str) -> list[str]:
    return sorted({str(row.get(field) or "") for row in rows if str(row.get(field) or "")})


def _unique(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _retargeting_method(candidate_id: str, dominant_failure_mode: str) -> str:
    if candidate_id == "baseline_plus_trend_structure":
        if dominant_failure_mode == "NEUTRAL_BAND_TOO_WIDE":
            return "neutral_band_narrowing_plus_direction_aware_scaling"
        return "piecewise_direction_aware_trend_scaling"
    if candidate_id == "risk_appetite":
        return "core_proxy_agreement_weighted_confidence_scaling"
    if dominant_failure_mode == "VOLATILITY_DAMPENING_TOO_STRONG":
        return "realized_volatility_percentile_scaling"
    return "cap_relaxation_plus_volatility_percentile_scaling"


def _missing_input_penalty(proposal: Mapping[str, Any], base_penalty: float) -> float:
    if str(proposal.get("proposal_type")) == "MISSING_INPUT_PENALTY_RECALIBRATION":
        return min(base_penalty, 0.12)
    return base_penalty


def _regime_scope(proposal: Mapping[str, Any]) -> list[str]:
    proposal_type = str(proposal.get("proposal_type"))
    if proposal_type == "REGIME_AWARE_SCALING":
        return ["high_volatility", "drawdown", "stress"]
    if proposal_type == "QUANTILE_BASED_SCALING":
        return ["low_volatility", "high_volatility"]
    return ["all_regimes"]


def _guardrail_profile(proposal: Mapping[str, Any]) -> str:
    proposal_type = str(proposal.get("proposal_type"))
    if proposal_type in {"CONFIDENCE_CAP_RELAXATION", "REGIME_AWARE_SCALING"}:
        return "strict_false_risk_off_and_overlap_stop"
    if proposal_type in {"DIRECTION_AWARE_SCALING", "NEUTRAL_BAND_NARROWING"}:
        return "strict_false_signal_cost_stop"
    return "balanced_distribution_retarget_guardrail"


def _expected_benefit(proposal: Mapping[str, Any]) -> str:
    proposal_type = str(proposal.get("proposal_type"))
    if proposal_type == "MISSING_INPUT_PENALTY_RECALIBRATION":
        return (
            "Recover high-conviction records when core proxies agree and optional "
            "proxy gaps remain disclosed."
        )
    if proposal_type == "CONFIDENCE_CAP_RELAXATION":
        return (
            "Allow stress or volatility expansion records to reach high-conviction evidence bands."
        )
    if proposal_type == "NEUTRAL_BAND_NARROWING":
        return "Reduce over-neutralization while preserving explicit neutral records."
    if proposal_type == "HORIZON_SPECIFIC_SCALING":
        return "Expose horizon-specific confidence differences hidden by aggregate scoring."
    if proposal_type == "ASSET_SPECIFIC_SCALING":
        return "Reduce cross-asset dilution, especially for semiconductor-specific risk appetite."
    if proposal_type == "QUANTILE_BASED_SCALING":
        return "Use local distribution distance to separate volatility compression and expansion."
    return "Improve low/medium/high conviction separation without changing source signal semantics."


def _expected_risk(proposal: Mapping[str, Any], dominant_false_cost: str) -> str:
    proposal_type = str(proposal.get("proposal_type"))
    risks = []
    if dominant_false_cost == "FALSE_RISK_ON_COST_TOO_HIGH":
        risks.append("false_risk_on_cost_amplification")
    if dominant_false_cost == "FALSE_RISK_OFF_COST_TOO_HIGH":
        risks.append("false_risk_off_cost_amplification")
    if proposal_type in {"CONFIDENCE_CAP_RELAXATION", "REGIME_AWARE_SCALING"}:
        risks.append("stress_or_risk_off_overconfidence")
    if proposal_type in {"ASSET_SPECIFIC_SCALING", "HORIZON_SPECIFIC_SCALING"}:
        risks.append("subset_overfit")
    return ", ".join(risks) if risks else "overconfidence_without_alignment_gain"


def _risk_on_failure_mode(dominant_false_cost: str) -> str:
    if dominant_false_cost == "FALSE_RISK_ON_COST_TOO_HIGH":
        return "risk_on_confidence_may_amplify_drawdown_or_stress_errors"
    return "monitor_false_risk_on_cost_after_confidence_lift"


def _risk_off_failure_mode(dominant_false_cost: str) -> str:
    if dominant_false_cost == "FALSE_RISK_OFF_COST_TOO_HIGH":
        return "risk_off_confidence_may_amplify_missed_upside"
    return "monitor_false_risk_off_cost_after_confidence_lift"


def _validation_focus(proposal: Mapping[str, Any], dominant_false_cost: str) -> list[str]:
    focus = ["high_confidence_alignment"]
    proposal_type = str(proposal.get("proposal_type"))
    if dominant_false_cost == "FALSE_RISK_ON_COST_TOO_HIGH":
        focus.append("false_risk_on_cost")
    if dominant_false_cost == "FALSE_RISK_OFF_COST_TOO_HIGH":
        focus.append("false_risk_off_cost")
    if proposal_type == "HORIZON_SPECIFIC_SCALING":
        focus.append("horizon_specific_alignment")
    if proposal_type == "ASSET_SPECIFIC_SCALING":
        focus.append("asset_specific_alignment")
    if proposal_type in {"REGIME_AWARE_SCALING", "QUANTILE_BASED_SCALING"}:
        focus.append("regime_specific_alignment")
    if proposal_type in {"ASSET_SPECIFIC_SCALING", "REGIME_AWARE_SCALING"}:
        focus.append("candidate_overlap")
    return _unique(focus)


def _overlap_labels_by_candidate(
    overlap_rows: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    labels: dict[str, str] = {}
    for row in overlap_rows:
        label = str(row.get("diagnostic_label") or "UNKNOWN_OVERLAP")
        for field in ("candidate_id_left", "candidate_id_right"):
            candidate_id = str(row.get(field) or "")
            if not candidate_id:
                continue
            if label == "HIGHLY_REDUNDANT":
                labels[candidate_id] = "high_overlap_risk"
            elif candidate_id not in labels:
                labels[candidate_id] = label.lower()
    return labels


def _implementation_scope(diagnosis: Mapping[str, Any]) -> str:
    dominant = str(diagnosis.get("dominant_failure_mode") or "")
    secondary = {str(item) for item in diagnosis.get("secondary_failure_modes", [])}
    if dominant == "DIRECTIONAL_SCORE_TOO_WEAK" and "LOW_CONFIDENCE_SIGNAL" not in secondary:
        return "CONFIDENCE_AND_DIRECTION_MAPPING"
    if dominant == "CROSS_ASSET_AVERAGING_DILUTES_SIGNAL":
        return "CONFIDENCE_AND_ASSET_SCOPE"
    if dominant == "HORIZON_AVERAGING_DILUTES_SIGNAL":
        return "CONFIDENCE_AND_HORIZON_SCOPE"
    return "CONFIDENCE_SCALING_ONLY"


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)
