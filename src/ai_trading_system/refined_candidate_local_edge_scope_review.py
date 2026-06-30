from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    round_float,
    to_float,
    write_json,
    write_markdown,
    write_matrix_artifacts,
)
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_REFINED_GENERATOR_ROOT,
    DEFAULT_REFINEMENT_PLAN_ROOT,
)
from ai_trading_system.refined_candidate_generators_regenerate import REFINED_CANDIDATE_IDS
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list

DEFAULT_REFINED_VALIDATION_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "refined_candidate_actual_path_validation"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "refined_candidate_local_edge_scope_review"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2290_REFINED_CANDIDATE_LOCAL_EDGE_SCOPE_NARROWING_REVIEW"
STATUS = "LOCAL_EDGE_SCOPE_REVIEW_READY_PROMOTION_BLOCKED"
MODE = "local_edge_scope_review"
ARTIFACT_ROLE = "refined_candidate_local_edge_scope_review"
DEFAULT_CANDIDATES = tuple(REFINED_CANDIDATE_IDS.values())
DEFAULT_CONTINUE_RESEARCH_CANDIDATES = (
    "baseline_plus_trend_structure_refined_confidence_v1",
    "volatility_regime_refined_confidence_v1",
)
DEFAULT_REJECT_CANDIDATES = ("risk_appetite_refined_confidence_v1",)
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")

REQUIRED_VALIDATION_FILES = {
    "summary": "refined_candidate_actual_path_validation_summary.json",
    "outcome_csv": "refined_candidate_prediction_outcome_matrix.csv",
    "scorecard": "refined_candidate_validation_scorecard.json",
    "high_conviction": "refined_high_conviction_outcome_drilldown.json",
    "false_cost": "refined_false_signal_cost_matrix.json",
    "guardrail": "refined_guardrail_validation_matrix.json",
    "comparison": "original_vs_refined_actual_path_comparison.json",
    "state_recommendation": "refined_candidate_state_recommendation_matrix.json",
    "data_quality": "refined_candidate_data_quality_report.json",
}
REQUIRED_GENERATOR_TOP_LEVEL_FILES = {
    "run_summary": "refined_regeneration_run_summary.json",
    "validation_summary": "refined_regeneration_validation_summary.json",
    "delta_summary": "refined_original_vs_refined_delta_summary.json",
}
REQUIRED_GENERATOR_CANDIDATE_FILES = {
    "signal_spec": "refined_candidate_signal_spec.json",
    "signal_series": "refined_candidate_signal_series.csv",
    "prediction_artifact": "refined_candidate_prediction_artifact.json",
    "generation_summary": "refined_generation_summary.json",
    "validation_summary": "refined_validation_summary.json",
    "parameter_application_report": "refined_parameter_application_report.json",
    "original_vs_refined_delta": "refined_original_vs_refined_delta.json",
}
REQUIRED_REFINEMENT_PLAN_FILES = {
    "summary": "confidence_scaling_refinement_summary.json",
    "proposal_matrix": "candidate_confidence_scaling_proposal_matrix.json",
    "parameter_grid": "candidate_confidence_scaling_parameter_grid.json",
    "guardrail_matrix": "candidate_guardrail_matrix.json",
    "expected_risk_impact": "candidate_expected_risk_impact_matrix.json",
    "implementation_plan": "candidate_2288_implementation_plan.json",
}

BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}

# Research-only pilot thresholds for TRADING-2290 scope review. These label
# weak local evidence for follow-up research only; they never open promotion,
# paper-shadow, production or broker paths.
LOCAL_SCOPE_MINIMUM_ELIGIBLE_RECORDS = 1000
LOCAL_EDGE_HIGH_CONVICTION_ALIGNMENT_MIN = 0.50
LOCAL_EDGE_ALIGNMENT_DELTA_MIN = 0.05
LOCAL_EDGE_CONFIDENCE_WEIGHTED_SCORE_MIN = 0.0
FALSE_COST_PER_ELIGIBLE_BLOCK_THRESHOLD = 0.05
FALSE_COST_DELTA_WORSE_THRESHOLD = 0.0

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "artifact_role": ARTIFACT_ROLE,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "owner_review_required": False,
    "paper_shadow_recommendation_allowed": False,
    "production_recommendation_allowed": False,
    "broker_action_recommendation_allowed": False,
    "promotion_eligible": False,
    "trading_2281_permanently_inconclusive_decisions_changed": False,
    "trading_2285_original_inconclusive_decisions_changed": False,
    "trading_2289_refined_state_decisions_changed": False,
}


class RefinedCandidateScopeReviewError(ValueError):
    pass


@dataclass(frozen=True)
class RefinedCandidateScopeReviewInputs:
    refined_validation_dir: Path
    refined_generator_dir: Path
    refinement_plan_dir: Path
    candidates: tuple[str, ...]
    continue_research_candidates: tuple[str, ...]
    reject_candidates: tuple[str, ...]
    validation_payloads: dict[str, dict[str, Any]]
    generator_payloads: dict[str, dict[str, Any]]
    generator_candidate_payloads: dict[str, dict[str, dict[str, Any]]]
    refinement_plan_payloads: dict[str, dict[str, Any]]
    outcome_rows: list[dict[str, Any]]
    warnings: list[str]

    @property
    def summary(self) -> dict[str, Any]:
        payload = self.validation_payloads["summary"]
        summary = payload.get("summary")
        return dict(summary) if isinstance(summary, Mapping) else {}

    @property
    def scorecards(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.validation_payloads["scorecard"], "candidate_scorecards")

    @property
    def high_conviction_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.validation_payloads["high_conviction"], "rows")

    @property
    def false_cost_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.validation_payloads["false_cost"], "rows")

    @property
    def guardrail_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.validation_payloads["guardrail"], "rows")

    @property
    def comparison_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.validation_payloads["comparison"], "rows")

    @property
    def state_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(
            self.validation_payloads["state_recommendation"], "candidate_rows"
        )

    @property
    def data_quality_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.validation_payloads["data_quality"], "candidate_rows")


def run_refined_candidate_local_edge_scope_review(
    *,
    refined_validation_dir: Path = DEFAULT_REFINED_VALIDATION_ROOT,
    refined_generator_dir: Path = DEFAULT_REFINED_GENERATOR_ROOT,
    refinement_plan_dir: Path = DEFAULT_REFINEMENT_PLAN_ROOT,
    candidates: Sequence[str] | str = DEFAULT_CANDIDATES,
    continue_research_candidates: Sequence[str] | str = DEFAULT_CONTINUE_RESEARCH_CANDIDATES,
    reject_candidates: Sequence[str] | str = DEFAULT_REJECT_CANDIDATES,
    target_assets: Sequence[str] | str = DEFAULT_TARGET_ASSETS,
    horizons: Sequence[str] | str = DEFAULT_HORIZONS,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise RefinedCandidateScopeReviewError(
            "refined candidate local-edge scope review only supports local_edge_scope_review mode"
        )
    candidate_ids = _normalize_list(candidates)
    continue_ids = _normalize_list(continue_research_candidates)
    reject_ids = _normalize_list(reject_candidates)
    asset_ids = set(_normalize_list(target_assets))
    horizon_ids = set(_normalize_list(horizons))
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    inputs = load_refined_candidate_scope_review_inputs(
        refined_validation_dir=refined_validation_dir,
        refined_generator_dir=refined_generator_dir,
        refinement_plan_dir=refinement_plan_dir,
        candidates=candidate_ids,
        continue_research_candidates=continue_ids,
        reject_candidates=reject_ids,
    )
    outcome_rows = [
        row
        for row in inputs.outcome_rows
        if str(row.get("refined_candidate_id")) in set(candidate_ids)
        and str(row.get("target_asset")) in asset_ids
        and str(row.get("horizon")) in horizon_ids
    ]
    if not outcome_rows:
        raise RefinedCandidateScopeReviewError("no eligible outcome rows for requested scope")

    local_edge_rows = build_candidate_local_edge_matrix(
        scorecards=inputs.scorecards,
        state_rows=inputs.state_rows,
        high_conviction_rows=inputs.high_conviction_rows,
        comparison_rows=inputs.comparison_rows,
        guardrail_rows=inputs.guardrail_rows,
        data_quality_rows=inputs.data_quality_rows,
        candidates=candidate_ids,
        reject_candidates=reject_ids,
    )
    asset_rows = build_candidate_asset_scope_matrix(outcome_rows, inputs.guardrail_rows)
    horizon_rows = build_candidate_horizon_scope_matrix(outcome_rows, inputs.guardrail_rows)
    direction_rows = build_candidate_direction_scope_matrix(outcome_rows)
    high_conviction_scope_rows = build_candidate_high_conviction_scope_matrix(outcome_rows)
    regime_rows = build_candidate_regime_scope_matrix(outcome_rows)
    false_cost_scope_rows = build_candidate_false_cost_scope_matrix(
        asset_rows=asset_rows,
        horizon_rows=horizon_rows,
        direction_rows=direction_rows,
        high_conviction_rows=high_conviction_scope_rows,
        regime_rows=regime_rows,
    )
    risk_appetite_record = build_risk_appetite_reject_record(
        state_rows=inputs.state_rows,
        comparison_rows=inputs.comparison_rows,
        reject_candidates=reject_ids,
    )
    scope_rows = build_candidate_scope_narrowing_recommendation_matrix(
        local_edge_rows=local_edge_rows,
        asset_rows=asset_rows,
        horizon_rows=horizon_rows,
        direction_rows=direction_rows,
        high_conviction_rows=high_conviction_scope_rows,
        regime_rows=regime_rows,
        reject_candidates=reject_ids,
    )
    next_task_rows = build_candidate_next_task_recommendation_matrix(scope_rows)
    decision_summary = build_candidate_scope_review_decision_summary(
        scope_rows=scope_rows,
        next_task_rows=next_task_rows,
        local_edge_rows=local_edge_rows,
        warnings=inputs.warnings,
    )
    summary = build_local_edge_scope_review_summary(
        inputs=inputs,
        local_edge_rows=local_edge_rows,
        scope_rows=scope_rows,
        next_task_rows=next_task_rows,
        risk_appetite_record=risk_appetite_record,
        generated_at=generated_at,
        mode=mode,
    )

    common = _common_payload(generated_at=generated_at, mode=mode)
    paths = _output_paths(output_dir, docs_root)
    _write_outputs(
        paths=paths,
        common=common,
        summary=summary,
        local_edge_rows=local_edge_rows,
        asset_rows=asset_rows,
        horizon_rows=horizon_rows,
        direction_rows=direction_rows,
        high_conviction_scope_rows=high_conviction_scope_rows,
        regime_rows=regime_rows,
        false_cost_scope_rows=false_cost_scope_rows,
        scope_rows=scope_rows,
        risk_appetite_record=risk_appetite_record,
        next_task_rows=next_task_rows,
        decision_summary=decision_summary,
    )

    payload = {
        **common,
        "status": STATUS,
        "summary": summary["summary"],
        "candidate_local_edge_rows": local_edge_rows,
        "candidate_scope_recommendation_rows": scope_rows,
        "candidate_next_task_recommendation_rows": next_task_rows,
        "risk_appetite_reject_record": risk_appetite_record,
    }
    _assert_generated_payload_safe("refined_candidate_local_edge_scope_review", payload)
    return payload


def load_refined_candidate_scope_review_inputs(
    *,
    refined_validation_dir: Path,
    refined_generator_dir: Path,
    refinement_plan_dir: Path,
    candidates: Sequence[str] | str,
    continue_research_candidates: Sequence[str] | str,
    reject_candidates: Sequence[str] | str,
) -> RefinedCandidateScopeReviewInputs:
    candidate_ids = _normalize_list(candidates)
    continue_ids = _normalize_list(continue_research_candidates)
    reject_ids = _normalize_list(reject_candidates)
    if not continue_ids:
        raise RefinedCandidateScopeReviewError("continue-research candidates are required")
    missing_continue = sorted(set(continue_ids) - set(candidate_ids))
    if missing_continue:
        raise RefinedCandidateScopeReviewError(
            f"continue-research candidates missing from requested candidates: {missing_continue}"
        )

    validation_payloads: dict[str, dict[str, Any]] = {}
    for key, filename in REQUIRED_VALIDATION_FILES.items():
        path = refined_validation_dir / filename
        if key == "outcome_csv":
            if not path.exists():
                raise RefinedCandidateScopeReviewError(f"missing refined validation file: {path}")
            continue
        validation_payloads[key] = _load_json_required(path, f"refined_validation.{key}")

    outcome_rows = _load_outcome_csv(
        refined_validation_dir / REQUIRED_VALIDATION_FILES["outcome_csv"]
    )
    _assert_outcome_rows_safe(outcome_rows)

    generator_payloads = {
        key: _load_json_required(refined_generator_dir / filename, f"refined_generator.{key}")
        for key, filename in REQUIRED_GENERATOR_TOP_LEVEL_FILES.items()
    }
    generator_candidate_payloads: dict[str, dict[str, dict[str, Any]]] = {}
    for candidate_id in candidate_ids:
        candidate_dir = refined_generator_dir / candidate_id
        if not candidate_dir.exists():
            raise RefinedCandidateScopeReviewError(
                f"missing refined candidate dir: {candidate_dir}"
            )
        generator_candidate_payloads[candidate_id] = {}
        for key, filename in REQUIRED_GENERATOR_CANDIDATE_FILES.items():
            path = candidate_dir / filename
            if filename.endswith(".csv"):
                if not path.exists():
                    raise RefinedCandidateScopeReviewError(
                        f"missing refined generator file: {path}"
                    )
                continue
            generator_candidate_payloads[candidate_id][key] = _load_json_required(
                path,
                f"refined_generator.{candidate_id}.{key}",
            )

    refinement_plan_payloads = {
        key: _load_json_required(refinement_plan_dir / filename, f"refinement_plan.{key}")
        for key, filename in REQUIRED_REFINEMENT_PLAN_FILES.items()
    }

    warnings = _review_warnings(
        validation_payloads=validation_payloads,
        continue_research_candidates=continue_ids,
        reject_candidates=reject_ids,
    )

    return RefinedCandidateScopeReviewInputs(
        refined_validation_dir=refined_validation_dir,
        refined_generator_dir=refined_generator_dir,
        refinement_plan_dir=refinement_plan_dir,
        candidates=tuple(candidate_ids),
        continue_research_candidates=tuple(continue_ids),
        reject_candidates=tuple(reject_ids),
        validation_payloads=validation_payloads,
        generator_payloads=generator_payloads,
        generator_candidate_payloads=generator_candidate_payloads,
        refinement_plan_payloads=refinement_plan_payloads,
        outcome_rows=outcome_rows,
        warnings=warnings,
    )


def build_candidate_local_edge_matrix(
    *,
    scorecards: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    high_conviction_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
    candidates: Sequence[str],
    reject_candidates: Sequence[str] = (),
) -> list[dict[str, Any]]:
    score_by_candidate = _rows_by_candidate(scorecards)
    state_by_candidate = _rows_by_candidate(state_rows)
    high_by_candidate = _aggregate_high_conviction_rows_by_candidate(high_conviction_rows)
    comparison_by_candidate = _rows_by_candidate(comparison_rows)
    guardrail_by_candidate = _rows_by_candidate(guardrail_rows)
    data_quality_by_candidate = _rows_by_candidate(data_quality_rows)
    reject_set = set(reject_candidates)
    rows: list[dict[str, Any]] = []
    for candidate_id in candidates:
        score = score_by_candidate.get(candidate_id, {})
        state = state_by_candidate.get(candidate_id, {})
        comparison = comparison_by_candidate.get(candidate_id, {})
        high = high_by_candidate.get(candidate_id, {})
        data_quality = data_quality_by_candidate.get(candidate_id, {})
        guardrail = guardrail_by_candidate.get(candidate_id, {})
        original_id = str(
            score.get("original_candidate_id")
            or state.get("original_candidate_id")
            or comparison.get("original_candidate_id")
            or ""
        )
        high_alignment = to_float(
            score.get("high_conviction_alignment_rate")
            or high.get("high_conviction_alignment_rate")
        )
        overall_alignment = to_float(score.get("alignment_rate"))
        high_delta = round_float(high_alignment - overall_alignment)
        guardrail_status = str(
            score.get("guardrail_status")
            or guardrail.get("guardrail_status")
            or state.get("guardrail_status")
            or "UNKNOWN"
        )
        data_quality_status = str(
            data_quality.get("data_quality_status")
            or state.get("data_quality_status")
            or comparison.get("data_quality_status_refined")
            or "UNKNOWN"
        )
        label = classify_local_edge_label(
            research_status=str(state.get("recommended_research_status") or ""),
            guardrail_status=guardrail_status,
            data_quality_status=data_quality_status,
            high_conviction_eligible_count=int(
                to_float(score.get("high_conviction_eligible_record_count"))
            ),
            high_conviction_alignment_rate=high_alignment,
            high_vs_overall_alignment_delta=high_delta,
            high_conviction_confidence_weighted_score=to_float(
                score.get("high_conviction_confidence_weighted_alignment_score")
            ),
            confidence_weighted_score=to_float(score.get("confidence_weighted_alignment_score")),
            false_risk_on_cost_delta=to_float(comparison.get("false_risk_on_cost_delta")),
            false_risk_off_cost_delta=to_float(comparison.get("false_risk_off_cost_delta")),
            is_reject_candidate=candidate_id in reject_set,
        )
        rows.append(
            {
                "refined_candidate_id": candidate_id,
                "original_candidate_id": original_id,
                "research_status_from_2289": state.get("recommended_research_status"),
                "record_count": int(to_float(score.get("record_count"))),
                "eligible_record_count": int(
                    to_float(score.get("validation_eligible_record_count"))
                ),
                "alignment_rate": round_float(score.get("alignment_rate")),
                "weighted_alignment_score": round_float(score.get("weighted_alignment_score")),
                "confidence_weighted_score": round_float(
                    score.get("confidence_weighted_alignment_score")
                ),
                "high_conviction_record_count": int(
                    to_float(score.get("high_conviction_record_count"))
                ),
                "high_conviction_eligible_count": int(
                    to_float(score.get("high_conviction_eligible_record_count"))
                ),
                "high_conviction_alignment_rate": round_float(high_alignment),
                "high_vs_overall_alignment_delta": high_delta,
                "false_risk_on_cost": round_float(score.get("false_risk_on_cost")),
                "false_risk_off_cost": round_float(score.get("false_risk_off_cost")),
                "guardrail_status": guardrail_status,
                "data_quality_status": data_quality_status,
                "local_edge_label": label,
                "primary_scope_opportunity": _primary_scope_opportunity(
                    original_id=original_id,
                    local_edge_label=label,
                    is_reject_candidate=candidate_id in reject_set,
                ),
                **_safety_subset(),
            }
        )
    return rows


def classify_local_edge_label(
    *,
    research_status: str,
    guardrail_status: str,
    data_quality_status: str,
    high_conviction_eligible_count: int,
    high_conviction_alignment_rate: float,
    high_vs_overall_alignment_delta: float,
    high_conviction_confidence_weighted_score: float,
    confidence_weighted_score: float,
    false_risk_on_cost_delta: float,
    false_risk_off_cost_delta: float,
    is_reject_candidate: bool = False,
) -> str:
    if data_quality_status == "FAIL":
        return "LOCAL_EDGE_DATA_QUALITY_BLOCKED"
    if guardrail_status == "FAIL":
        return "LOCAL_EDGE_GUARDRAIL_BLOCKED"
    if (
        false_risk_on_cost_delta > FALSE_COST_DELTA_WORSE_THRESHOLD
        or false_risk_off_cost_delta > FALSE_COST_DELTA_WORSE_THRESHOLD
    ):
        return "LOCAL_EDGE_FALSE_COST_BLOCKED"
    if is_reject_candidate or "REJECT" in research_status:
        return "LOCAL_EDGE_NOT_FOUND"
    if high_conviction_eligible_count >= LOCAL_SCOPE_MINIMUM_ELIGIBLE_RECORDS:
        if (
            high_conviction_alignment_rate >= LOCAL_EDGE_HIGH_CONVICTION_ALIGNMENT_MIN
            and high_vs_overall_alignment_delta >= LOCAL_EDGE_ALIGNMENT_DELTA_MIN
            and high_conviction_confidence_weighted_score > LOCAL_EDGE_CONFIDENCE_WEIGHTED_SCORE_MIN
        ):
            return "LOCAL_EDGE_PRESENT"
        if (
            high_vs_overall_alignment_delta >= LOCAL_EDGE_ALIGNMENT_DELTA_MIN
            and high_conviction_confidence_weighted_score > LOCAL_EDGE_CONFIDENCE_WEIGHTED_SCORE_MIN
        ):
            return "LOCAL_EDGE_WEAK"
    if confidence_weighted_score > LOCAL_EDGE_CONFIDENCE_WEIGHTED_SCORE_MIN:
        return "LOCAL_EDGE_WEAK"
    return "LOCAL_EDGE_NOT_FOUND"


def build_candidate_asset_scope_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    guardrail_by_candidate = _rows_by_candidate(guardrail_rows)
    rows = []
    for aggregate in _aggregate_outcome_scope(
        outcome_rows, ("refined_candidate_id", "target_asset")
    ):
        candidate_id = str(aggregate["refined_candidate_id"])
        label = _scope_label(prefix="ASSET", aggregate=aggregate)
        rows.append(
            {
                **aggregate,
                "guardrail_status": guardrail_by_candidate.get(candidate_id, {}).get(
                    "guardrail_status", "UNKNOWN"
                ),
                "scope_label": label,
            }
        )
    return rows


def build_candidate_horizon_scope_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    rows = []
    for aggregate in _aggregate_outcome_scope(outcome_rows, ("refined_candidate_id", "horizon")):
        rows.append(
            {**aggregate, "scope_label": _scope_label(prefix="HORIZON", aggregate=aggregate)}
        )
    return rows


def build_candidate_direction_scope_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for aggregate in _aggregate_outcome_scope(
        outcome_rows, ("refined_candidate_id", "signal_direction")
    ):
        rows.append(
            {
                **aggregate,
                "direction_scope_label": classify_direction_scope_label(aggregate),
            }
        )
    return rows


def classify_direction_scope_label(row: Mapping[str, Any]) -> str:
    direction = str(row.get("signal_direction") or "")
    candidate_id = str(row.get("refined_candidate_id") or "")
    score = to_float(row.get("confidence_weighted_score"))
    false_cost_blocked = _false_cost_per_record_blocked(row)
    eligible = int(to_float(row.get("eligible_record_count")))
    if eligible < LOCAL_SCOPE_MINIMUM_ELIGIBLE_RECORDS:
        return "DIRECTION_INCONCLUSIVE"
    if false_cost_blocked or score < 0.0:
        return "DIRECTION_DROP"
    if direction == "neutral":
        return "DIRECTION_INCONCLUSIVE"
    if "volatility_regime" in candidate_id and direction in {"risk_off", "volatility_expansion"}:
        return "DIRECTION_RISK_CAP_ONLY"
    if "baseline_plus_trend_structure" in candidate_id and direction in {
        "trend_confirming",
        "trend_weakening",
    }:
        return "DIRECTION_CONFIRMATION_ONLY"
    return "DIRECTION_KEEP"


def build_candidate_high_conviction_scope_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped = _group_rows(outcome_rows, ("refined_candidate_id",))
    rows: list[dict[str, Any]] = []
    for (candidate_id,), group_rows in grouped.items():
        original_id = _first_value(group_rows, "original_candidate_id")
        high_rows = [row for row in group_rows if _bool(row.get("high_conviction_flag"))]
        non_high_rows = [row for row in group_rows if not _bool(row.get("high_conviction_flag"))]
        high_eligible = _eligible_rows(high_rows)
        non_high_eligible = _eligible_rows(non_high_rows)
        high_alignment = _alignment_rate(high_eligible)
        non_high_alignment = _alignment_rate(non_high_eligible)
        false_on = _false_cost(high_eligible, "false_risk_on")
        false_off = _false_cost(high_eligible, "false_risk_off")
        row = {
            "refined_candidate_id": candidate_id,
            "original_candidate_id": original_id,
            "high_conviction_eligible_count": len(high_eligible),
            "high_conviction_alignment_rate": round_float(high_alignment),
            "non_high_conviction_alignment_rate": round_float(non_high_alignment),
            "high_vs_non_high_alignment_delta": round_float(high_alignment - non_high_alignment),
            "high_conviction_false_risk_on_cost": round_float(false_on),
            "high_conviction_false_risk_off_cost": round_float(false_off),
        }
        row["high_conviction_scope_label"] = classify_high_conviction_scope_label(row)
        rows.append(row)
    return rows


def classify_high_conviction_scope_label(row: Mapping[str, Any]) -> str:
    eligible = int(to_float(row.get("high_conviction_eligible_count")))
    if eligible < LOCAL_SCOPE_MINIMUM_ELIGIBLE_RECORDS:
        return "HIGH_CONVICTION_SCOPE_INCONCLUSIVE"
    false_cost = to_float(row.get("high_conviction_false_risk_on_cost")) + to_float(
        row.get("high_conviction_false_risk_off_cost")
    )
    if false_cost / max(eligible, 1) > FALSE_COST_PER_ELIGIBLE_BLOCK_THRESHOLD:
        return "HIGH_CONVICTION_FALSE_COST_BLOCKED"
    delta = to_float(row.get("high_vs_non_high_alignment_delta"))
    high_alignment = to_float(row.get("high_conviction_alignment_rate"))
    if high_alignment <= 0.0:
        return "HIGH_CONVICTION_SCOPE_DROP"
    if delta >= LOCAL_EDGE_ALIGNMENT_DELTA_MIN:
        return "HIGH_CONVICTION_SCOPE_KEEP_ONLY"
    if high_alignment >= LOCAL_EDGE_HIGH_CONVICTION_ALIGNMENT_MIN:
        return "HIGH_CONVICTION_SCOPE_KEEP_WITH_ALL"
    return "HIGH_CONVICTION_SCOPE_INCONCLUSIVE"


def build_candidate_regime_scope_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enriched = []
    for row in outcome_rows:
        enriched.append({**dict(row), "regime_label": diagnostic_regime_label(row)})
    rows = []
    for aggregate in _aggregate_outcome_scope(enriched, ("refined_candidate_id", "regime_label")):
        rows.append(
            {**aggregate, "regime_scope_label": _scope_label(prefix="REGIME", aggregate=aggregate)}
        )
    return rows


def diagnostic_regime_label(row: Mapping[str, Any]) -> str:
    driver = str(row.get("dominant_observed_driver") or "").lower()
    if "stress" in driver:
        return "stress"
    if "drawdown" in driver:
        return "drawdown"
    if "rebound" in driver:
        return "rebound"
    if "volatility" in driver and "low" not in driver:
        return "high_volatility"
    if "low_volatility" in driver:
        return "low_volatility"
    forward_return = to_float(row.get("actual_forward_return"))
    drawdown = to_float(row.get("actual_max_drawdown"))
    if forward_return > 0.0 and drawdown > -0.02:
        return "uptrend"
    if forward_return < 0.0 and drawdown < -0.03:
        return "downtrend"
    if forward_return == 0.0:
        return "unknown"
    return "range_bound"


def build_candidate_false_cost_scope_matrix(
    *,
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    direction_rows: Sequence[Mapping[str, Any]],
    high_conviction_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sources = [
        ("asset", "target_asset", asset_rows),
        ("horizon", "horizon", horizon_rows),
        ("direction", "signal_direction", direction_rows),
        (
            "high_conviction",
            "scope_value",
            _high_conviction_false_cost_source(high_conviction_rows),
        ),
        ("regime", "regime_label", regime_rows),
    ]
    for dimension, value_key, source_rows in sources:
        for source in source_rows:
            eligible = int(
                to_float(
                    source.get("eligible_record_count")
                    or source.get("high_conviction_eligible_count")
                )
            )
            false_on = to_float(
                source.get("false_risk_on_cost") or source.get("high_conviction_false_risk_on_cost")
            )
            false_off = to_float(
                source.get("false_risk_off_cost")
                or source.get("high_conviction_false_risk_off_cost")
            )
            label = classify_false_cost_label(
                eligible_record_count=eligible,
                false_risk_on_cost=false_on,
                false_risk_off_cost=false_off,
            )
            scope_value = source.get(value_key)
            rows.append(
                {
                    "refined_candidate_id": source.get("refined_candidate_id"),
                    "original_candidate_id": source.get("original_candidate_id"),
                    "scope_dimension": dimension,
                    "scope_value": scope_value,
                    "eligible_record_count": eligible,
                    "false_risk_on_cost": round_float(false_on),
                    "false_risk_off_cost": round_float(false_off),
                    "false_cost_asymmetry": round_float(false_on - false_off),
                    "false_cost_label": label,
                    "recommended_usage": _false_cost_recommended_usage(
                        source, dimension, str(scope_value or ""), label
                    ),
                }
            )
    return rows


def classify_false_cost_label(
    *,
    eligible_record_count: int,
    false_risk_on_cost: float,
    false_risk_off_cost: float,
) -> str:
    if eligible_record_count <= 0:
        return "FALSE_COST_INCONCLUSIVE"
    false_on_blocked = (
        false_risk_on_cost / eligible_record_count > FALSE_COST_PER_ELIGIBLE_BLOCK_THRESHOLD
    )
    false_off_blocked = (
        false_risk_off_cost / eligible_record_count > FALSE_COST_PER_ELIGIBLE_BLOCK_THRESHOLD
    )
    if false_on_blocked and false_off_blocked:
        return "BOTH_FALSE_COSTS_TOO_HIGH"
    if false_on_blocked:
        return "FALSE_RISK_ON_COST_TOO_HIGH"
    if false_off_blocked:
        return "FALSE_RISK_OFF_COST_TOO_HIGH"
    return "FALSE_COST_ACCEPTABLE"


def build_risk_appetite_reject_record(
    *,
    state_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
    reject_candidates: Sequence[str] = DEFAULT_REJECT_CANDIDATES,
) -> dict[str, Any]:
    reject_id = next(
        (candidate for candidate in reject_candidates if "risk_appetite" in candidate),
        "risk_appetite_refined_confidence_v1",
    )
    state = _rows_by_candidate(state_rows).get(reject_id, {})
    comparison = _rows_by_candidate(comparison_rows).get(reject_id, {})
    recommended_status = str(state.get("recommended_research_status") or "")
    warning = ""
    if recommended_status != "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED":
        warning = "risk_appetite was not reject recommended in TRADING-2289 inputs"
    return {
        "candidate_id": reject_id,
        "refined_candidate_id": reject_id,
        "original_candidate_id": state.get("original_candidate_id")
        or comparison.get("original_candidate_id")
        or "risk_appetite",
        "source_tasks": [
            "TRADING-2284",
            "TRADING-2285",
            "TRADING-2286",
            "TRADING-2287",
            "TRADING-2288",
            "TRADING-2289",
        ],
        "reject_reason": _risk_appetite_reject_reason(recommended_status, comparison),
        "reject_scope": "current_form",
        "recommended_future_action": "archive_current_form",
        "risk_appetite_concept_permanently_rejected": False,
        "future_reopen_condition": "new_inputs_or_candidate_family_redesign_required",
        "warning": warning,
        **_safety_subset(),
    }


def build_candidate_scope_narrowing_recommendation_matrix(
    *,
    local_edge_rows: Sequence[Mapping[str, Any]],
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    direction_rows: Sequence[Mapping[str, Any]],
    high_conviction_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
    reject_candidates: Sequence[str],
) -> list[dict[str, Any]]:
    reject_set = set(reject_candidates)
    asset_by_candidate = _group_by_candidate(asset_rows)
    horizon_by_candidate = _group_by_candidate(horizon_rows)
    direction_by_candidate = _group_by_candidate(direction_rows)
    high_by_candidate = _rows_by_candidate(high_conviction_rows)
    regime_by_candidate = _group_by_candidate(regime_rows)
    rows = []
    for local_edge in local_edge_rows:
        candidate_id = str(local_edge.get("refined_candidate_id"))
        original_id = str(local_edge.get("original_candidate_id"))
        kept_assets = _kept_values(
            asset_by_candidate.get(candidate_id, []), "target_asset", "scope_label"
        )
        dropped_assets = _dropped_values(
            asset_by_candidate.get(candidate_id, []), "target_asset", "scope_label"
        )
        kept_horizons = _kept_values(
            horizon_by_candidate.get(candidate_id, []), "horizon", "scope_label"
        )
        dropped_horizons = _dropped_values(
            horizon_by_candidate.get(candidate_id, []), "horizon", "scope_label"
        )
        kept_directions = _kept_values(
            direction_by_candidate.get(candidate_id, []),
            "signal_direction",
            "direction_scope_label",
        )
        dropped_directions = _dropped_values(
            direction_by_candidate.get(candidate_id, []),
            "signal_direction",
            "direction_scope_label",
        )
        kept_regimes = _kept_values(
            regime_by_candidate.get(candidate_id, []), "regime_label", "regime_scope_label"
        )
        high_scope = str(
            high_by_candidate.get(candidate_id, {}).get("high_conviction_scope_label") or ""
        )
        action = _recommended_scope_action(
            candidate_id=candidate_id,
            local_edge_label=str(local_edge.get("local_edge_label")),
            high_scope_label=high_scope,
            kept_assets=kept_assets,
            kept_horizons=kept_horizons,
            kept_directions=kept_directions,
            reject_set=reject_set,
        )
        usage = _usage_recommendation(
            original_candidate_id=original_id,
            candidate_id=candidate_id,
            action=action,
            kept_directions=kept_directions,
        )
        rows.append(
            {
                "refined_candidate_id": candidate_id,
                "original_candidate_id": original_id,
                "candidate_status_after_2289": local_edge.get("research_status_from_2289"),
                "scope_review_status": local_edge.get("local_edge_label"),
                "recommended_scope_action": action,
                "kept_assets": kept_assets,
                "dropped_assets": dropped_assets,
                "kept_horizons": kept_horizons,
                "dropped_horizons": dropped_horizons,
                "kept_directions": kept_directions,
                "dropped_directions": dropped_directions,
                "kept_regimes": kept_regimes,
                "high_conviction_scope_label": high_scope,
                "usage_recommendation": usage,
                "next_task_recommendation": _next_task_for_action(action),
                **_safety_subset(),
            }
        )
    return rows


def build_candidate_next_task_recommendation_matrix(
    scope_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for row in scope_rows:
        action = str(row.get("recommended_scope_action"))
        rows.append(
            {
                "refined_candidate_id": row.get("refined_candidate_id"),
                "original_candidate_id": row.get("original_candidate_id"),
                "recommended_scope_action": action,
                "usage_recommendation": row.get("usage_recommendation"),
                "next_task": _next_task_for_action(action),
                "next_task_rationale": _next_task_rationale(action),
                **_safety_subset(),
            }
        )
    return rows


def build_candidate_scope_review_decision_summary(
    *,
    scope_rows: Sequence[Mapping[str, Any]],
    next_task_rows: Sequence[Mapping[str, Any]],
    local_edge_rows: Sequence[Mapping[str, Any]],
    warnings: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        **_safety_subset(),
        "candidate_count": len(scope_rows),
        "local_edge_label_counts": dict(
            Counter(str(row.get("local_edge_label")) for row in local_edge_rows)
        ),
        "recommended_scope_action_counts": dict(
            Counter(str(row.get("recommended_scope_action")) for row in scope_rows)
        ),
        "next_task_counts": dict(Counter(str(row.get("next_task")) for row in next_task_rows)),
        "warnings": list(warnings),
        "candidate_decisions": list(scope_rows),
    }


def build_local_edge_scope_review_summary(
    *,
    inputs: RefinedCandidateScopeReviewInputs,
    local_edge_rows: Sequence[Mapping[str, Any]],
    scope_rows: Sequence[Mapping[str, Any]],
    next_task_rows: Sequence[Mapping[str, Any]],
    risk_appetite_record: Mapping[str, Any],
    generated_at: datetime,
    mode: str,
) -> dict[str, Any]:
    source_summary = inputs.summary
    owner_review_count = sum(
        1 for row in inputs.state_rows if _bool(row.get("owner_review_candidate_recommendation"))
    )
    scope_narrowing_count = sum(
        1
        for row in scope_rows
        if str(row.get("recommended_scope_action")).startswith("SCOPE_NARROW")
    )
    summary = {
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "input_actual_path_records_from_2289": int(
            to_float(source_summary.get("actual_path_record_count"))
        ),
        "input_eligible_records_from_2289": int(
            to_float(source_summary.get("validation_eligible_record_count"))
        ),
        "input_data_quality_status_from_2289": source_summary.get("data_quality_status"),
        "candidate_count": len(inputs.candidates),
        "continue_research_candidate_count": len(inputs.continue_research_candidates),
        "reject_candidate_count": len(inputs.reject_candidates),
        "owner_review_candidate_count": owner_review_count,
        "scope_narrowing_candidate_count": scope_narrowing_count,
        "risk_appetite_reject_record_generated": bool(risk_appetite_record),
        "local_edge_label_counts": dict(
            Counter(str(row.get("local_edge_label")) for row in local_edge_rows)
        ),
        "recommended_scope_action_counts": dict(
            Counter(str(row.get("recommended_scope_action")) for row in scope_rows)
        ),
        "next_task_recommendation": sorted(
            {str(row.get("next_task")) for row in next_task_rows if row.get("next_task")}
        ),
        "warnings": list(inputs.warnings),
        **_safety_subset(),
    }
    return {
        **_common_payload(generated_at=generated_at, mode=mode),
        "status": STATUS,
        "summary": summary,
        "summary_status": STATUS,
    }


def _write_outputs(
    *,
    paths: Mapping[str, Path],
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    local_edge_rows: Sequence[Mapping[str, Any]],
    asset_rows: Sequence[Mapping[str, Any]],
    horizon_rows: Sequence[Mapping[str, Any]],
    direction_rows: Sequence[Mapping[str, Any]],
    high_conviction_scope_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
    false_cost_scope_rows: Sequence[Mapping[str, Any]],
    scope_rows: Sequence[Mapping[str, Any]],
    risk_appetite_record: Mapping[str, Any],
    next_task_rows: Sequence[Mapping[str, Any]],
    decision_summary: Mapping[str, Any],
) -> None:
    write_json(paths["summary"], summary)
    write_matrix_artifacts(
        paths["local_edge_json"], paths["local_edge_csv"], common, local_edge_rows
    )
    write_matrix_artifacts(paths["asset_json"], paths["asset_csv"], common, asset_rows)
    write_matrix_artifacts(paths["horizon_json"], paths["horizon_csv"], common, horizon_rows)
    write_matrix_artifacts(paths["direction_json"], paths["direction_csv"], common, direction_rows)
    write_matrix_artifacts(
        paths["high_conviction_json"],
        paths["high_conviction_csv"],
        common,
        high_conviction_scope_rows,
    )
    write_matrix_artifacts(paths["regime_json"], paths["regime_csv"], common, regime_rows)
    write_matrix_artifacts(
        paths["false_cost_json"],
        paths["false_cost_csv"],
        common,
        false_cost_scope_rows,
    )
    write_matrix_artifacts(paths["scope_json"], paths["scope_csv"], common, scope_rows)
    write_json(paths["risk_appetite_json"], {**common, **dict(risk_appetite_record)})
    write_markdown(
        paths["risk_appetite_runtime_md"], _render_risk_appetite_reject_doc(risk_appetite_record)
    )
    write_json(paths["next_task_json"], {**common, "rows": list(next_task_rows)})
    write_json(paths["decision_summary_json"], {**common, "summary": dict(decision_summary)})

    write_markdown(
        paths["local_edge_doc"], _render_local_edge_doc(summary["summary"], local_edge_rows)
    )
    write_markdown(paths["scope_doc"], _render_scope_recommendation_doc(scope_rows))
    write_markdown(
        paths["risk_appetite_doc"], _render_risk_appetite_reject_doc(risk_appetite_record)
    )
    write_markdown(paths["next_task_doc"], _render_next_task_doc(next_task_rows))


def _aggregate_outcome_scope(
    outcome_rows: Sequence[Mapping[str, Any]],
    group_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = []
    for key, group_rows in _group_rows(outcome_rows, group_fields).items():
        eligible = _eligible_rows(group_rows)
        high_eligible = [row for row in eligible if _bool(row.get("high_conviction_flag"))]
        aggregate = {field: key[index] for index, field in enumerate(group_fields)}
        aggregate.update(
            {
                "original_candidate_id": _first_value(group_rows, "original_candidate_id"),
                "eligible_record_count": len(eligible),
                "alignment_rate": round_float(_alignment_rate(eligible)),
                "weighted_alignment_score": round_float(_weighted_alignment_score(eligible)),
                "confidence_weighted_score": round_float(_confidence_weighted_score(eligible)),
                "high_conviction_alignment_rate": round_float(_alignment_rate(high_eligible)),
                "false_risk_on_cost": round_float(_false_cost(eligible, "false_risk_on")),
                "false_risk_off_cost": round_float(_false_cost(eligible, "false_risk_off")),
            }
        )
        rows.append(aggregate)
    return sorted(rows, key=lambda row: tuple(str(row.get(field, "")) for field in group_fields))


def _scope_label(*, prefix: str, aggregate: Mapping[str, Any]) -> str:
    eligible = int(to_float(aggregate.get("eligible_record_count")))
    if _false_cost_per_record_blocked(aggregate):
        return f"{prefix}_SCOPE_FALSE_COST_BLOCKED"
    if eligible < LOCAL_SCOPE_MINIMUM_ELIGIBLE_RECORDS:
        return f"{prefix}_SCOPE_INCONCLUSIVE"
    score = to_float(aggregate.get("confidence_weighted_score"))
    if score > 0.0:
        return f"{prefix}_SCOPE_KEEP"
    if score < 0.0:
        return f"{prefix}_SCOPE_DROP"
    return f"{prefix}_SCOPE_INCONCLUSIVE"


def _false_cost_per_record_blocked(row: Mapping[str, Any]) -> bool:
    eligible = int(to_float(row.get("eligible_record_count")))
    if eligible <= 0:
        return False
    total = to_float(row.get("false_risk_on_cost")) + to_float(row.get("false_risk_off_cost"))
    return total / eligible > FALSE_COST_PER_ELIGIBLE_BLOCK_THRESHOLD


def _false_cost(row: Sequence[Mapping[str, Any]], error_type: str) -> float:
    total = 0.0
    for item in row:
        if str(item.get("error_type")) != error_type:
            continue
        total += abs(to_float(item.get("actual_forward_return"))) + abs(
            to_float(item.get("actual_max_drawdown"))
        )
    return total


def _alignment_rate(rows: Sequence[Mapping[str, Any]]) -> float:
    eligible = _eligible_rows(rows)
    if not eligible:
        return 0.0
    aligned = sum(1 for row in eligible if to_float(row.get("alignment_score")) > 0.0)
    return aligned / len(eligible)


def _weighted_alignment_score(rows: Sequence[Mapping[str, Any]]) -> float:
    eligible = _eligible_rows(rows)
    if not eligible:
        return 0.0
    return sum(to_float(row.get("alignment_score")) for row in eligible) / len(eligible)


def _confidence_weighted_score(rows: Sequence[Mapping[str, Any]]) -> float:
    eligible = _eligible_rows(rows)
    if not eligible:
        return 0.0
    total = 0.0
    for row in eligible:
        confidence = to_float(
            row.get("refined_signal_confidence") or row.get("signal_confidence") or 0.0
        )
        total += to_float(row.get("alignment_score")) * confidence
    return total / len(eligible)


def _eligible_rows(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [row for row in rows if _bool(row.get("validation_eligible"))]


def _load_json_required(path: Path, name: str) -> dict[str, Any]:
    if not path.exists():
        raise RefinedCandidateScopeReviewError(f"missing required input: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RefinedCandidateScopeReviewError(f"{path} must contain a JSON object")
    _assert_input_payload_safe(name, payload)
    return payload


def _load_outcome_csv(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_csv(path)
    required = {
        "refined_candidate_id",
        "original_candidate_id",
        "target_asset",
        "horizon",
        "signal_direction",
        "validation_eligible",
        "alignment_score",
        "error_type",
        "actual_forward_return",
        "actual_max_drawdown",
        "refined_signal_confidence",
        "high_conviction_flag",
        "broker_action",
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
    }
    missing = required - set(frame.columns)
    if missing:
        raise RefinedCandidateScopeReviewError(
            f"refined outcome CSV missing columns: {sorted(missing)}"
        )
    return [dict(row) for row in frame.to_dict(orient="records")]


def _assert_input_payload_safe(name: str, payload: Any) -> None:
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if key in {
                "promotion_allowed",
                "paper_shadow_allowed",
                "production_allowed",
                "paper_shadow_recommendation_allowed",
                "production_recommendation_allowed",
                "broker_action_recommendation_allowed",
            } and _bool(value):
                raise RefinedCandidateScopeReviewError(f"{name}: {key} must remain false")
            if key == "broker_action" and str(value) not in {"", "none", "None"}:
                raise RefinedCandidateScopeReviewError(f"{name}: broker_action must remain none")
            if isinstance(value, str) and value in BANNED_RECOMMENDATIONS:
                raise RefinedCandidateScopeReviewError(f"{name}: banned recommendation {value}")
            _assert_input_payload_safe(f"{name}.{key}", value)
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            _assert_input_payload_safe(f"{name}[{index}]", item)


def _assert_outcome_rows_safe(rows: Sequence[Mapping[str, Any]]) -> None:
    for row in rows:
        for key in ("promotion_allowed", "paper_shadow_allowed", "production_allowed"):
            if _bool(row.get(key)):
                raise RefinedCandidateScopeReviewError(f"outcome row {key} must remain false")
        if str(row.get("broker_action")) not in {"", "none", "None", "nan"}:
            raise RefinedCandidateScopeReviewError("outcome row broker_action must remain none")


def _assert_generated_payload_safe(name: str, payload: Mapping[str, Any]) -> None:
    _assert_input_payload_safe(name, payload)


def _review_warnings(
    *,
    validation_payloads: Mapping[str, Mapping[str, Any]],
    continue_research_candidates: Sequence[str],
    reject_candidates: Sequence[str],
) -> list[str]:
    state_rows = _rows_from_payload(validation_payloads["state_recommendation"], "candidate_rows")
    state_by_candidate = _rows_by_candidate(state_rows)
    warnings: list[str] = []
    for candidate_id in continue_research_candidates:
        if candidate_id not in state_by_candidate:
            raise RefinedCandidateScopeReviewError(
                f"continue-research candidate missing from state recommendation: {candidate_id}"
            )
        status = str(state_by_candidate[candidate_id].get("recommended_research_status"))
        if status != "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH":
            raise RefinedCandidateScopeReviewError(
                f"continue-research candidate has incompatible 2289 status: {candidate_id}={status}"
            )
    for candidate_id in reject_candidates:
        if candidate_id not in state_by_candidate:
            raise RefinedCandidateScopeReviewError(
                f"reject candidate missing from state recommendation: {candidate_id}"
            )
        status = str(state_by_candidate[candidate_id].get("recommended_research_status"))
        if status != "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED":
            warnings.append(f"{candidate_id}: expected reject recommended, got {status}")
    return warnings


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    rows = payload.get(key)
    if not isinstance(rows, list) or not rows:
        raise RefinedCandidateScopeReviewError(f"missing or empty rows: {key}")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _rows_by_candidate(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate_id = str(
            row.get("refined_candidate_id") or row.get("candidate_id") or row.get("candidate") or ""
        )
        if candidate_id:
            result[candidate_id] = dict(row)
    return result


def _group_by_candidate(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("refined_candidate_id") or row.get("candidate_id"))].append(dict(row))
    return dict(grouped)


def _group_rows(
    rows: Sequence[Mapping[str, Any]],
    fields: tuple[str, ...],
) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(str(row.get(field) or "") for field in fields)
        grouped[key].append(dict(row))
    return dict(grouped)


def _aggregate_high_conviction_rows_by_candidate(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped = _group_by_candidate(rows)
    result: dict[str, dict[str, Any]] = {}
    for candidate_id, candidate_rows in grouped.items():
        eligible = sum(
            int(to_float(row.get("high_conviction_eligible_count"))) for row in candidate_rows
        )
        if eligible <= 0:
            result[candidate_id] = {
                "high_conviction_alignment_rate": 0.0,
                "high_conviction_eligible_count": 0,
            }
            continue
        weighted_alignment = sum(
            to_float(row.get("high_conviction_alignment_rate"))
            * int(to_float(row.get("high_conviction_eligible_count")))
            for row in candidate_rows
        )
        result[candidate_id] = {
            "high_conviction_alignment_rate": round_float(weighted_alignment / eligible),
            "high_conviction_eligible_count": eligible,
        }
    return result


def _first_value(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    for row in rows:
        value = row.get(key)
        if value not in {None, ""}:
            return str(value)
    return ""


def _high_conviction_false_cost_source(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            **dict(row),
            "scope_value": "high_conviction_only",
            "eligible_record_count": row.get("high_conviction_eligible_count"),
            "false_risk_on_cost": row.get("high_conviction_false_risk_on_cost"),
            "false_risk_off_cost": row.get("high_conviction_false_risk_off_cost"),
        }
        for row in rows
    ]


def _kept_values(rows: Sequence[Mapping[str, Any]], value_key: str, label_key: str) -> list[str]:
    return sorted(
        str(row.get(value_key))
        for row in rows
        if "KEEP" in str(row.get(label_key))
        or "CONFIRMATION_ONLY" in str(row.get(label_key))
        or "RISK_CAP_ONLY" in str(row.get(label_key))
    )


def _dropped_values(rows: Sequence[Mapping[str, Any]], value_key: str, label_key: str) -> list[str]:
    return sorted(str(row.get(value_key)) for row in rows if "DROP" in str(row.get(label_key)))


def _recommended_scope_action(
    *,
    candidate_id: str,
    local_edge_label: str,
    high_scope_label: str,
    kept_assets: Sequence[str],
    kept_horizons: Sequence[str],
    kept_directions: Sequence[str],
    reject_set: set[str],
) -> str:
    if candidate_id in reject_set:
        return "REJECT_CURRENT_FORM"
    if local_edge_label in {"LOCAL_EDGE_PRESENT", "LOCAL_EDGE_WEAK"}:
        if (
            high_scope_label == "HIGH_CONVICTION_SCOPE_KEEP_ONLY"
            or kept_directions
            or 0 < len(kept_assets) < len(DEFAULT_TARGET_ASSETS)
            or 0 < len(kept_horizons) < len(DEFAULT_HORIZONS)
        ):
            return "SCOPE_NARROW_AND_REGENERATE"
        return "FORWARD_OBSERVE_ONLY"
    if local_edge_label in {"LOCAL_EDGE_FALSE_COST_BLOCKED", "LOCAL_EDGE_NOT_FOUND"}:
        return "REDESIGN_CANDIDATE_FAMILY"
    return "FORWARD_OBSERVE_ONLY"


def _usage_recommendation(
    *,
    original_candidate_id: str,
    candidate_id: str,
    action: str,
    kept_directions: Sequence[str],
) -> str:
    if action == "REJECT_CURRENT_FORM":
        return "reject"
    if action == "REDESIGN_CANDIDATE_FAMILY":
        return "diagnostic_only"
    if "baseline_plus_trend_structure" in original_candidate_id or any(
        "trend" in direction for direction in kept_directions
    ):
        return "confirmation_only"
    if "volatility_regime" in original_candidate_id or "risk_off" in kept_directions:
        return "risk_cap_only"
    if "veto" in candidate_id:
        return "veto_only"
    return "diagnostic_only"


def _next_task_for_action(action: str) -> str:
    if action == "SCOPE_NARROW_AND_REGENERATE":
        return "TRADING-2291_Scope_Narrowed_Candidate_Regeneration"
    if action == "SCOPE_NARROW_AND_REVALIDATE_ONLY":
        return "TRADING-2291_Scope_Narrowed_Candidate_Revalidation_Only"
    if action == "REJECT_CURRENT_FORM":
        return "TRADING-2291_Archive_Rejected_Candidate_Current_Form"
    if action == "FORWARD_OBSERVE_ONLY":
        return "TRADING-2291_Forward_Observe_Local_Edge_Plan"
    return "TRADING-2291_Candidate_Family_Redesign_Plan"


def _next_task_rationale(action: str) -> str:
    if action == "SCOPE_NARROW_AND_REGENERATE":
        return (
            "local edge is limited to a narrower research scope and requires "
            "scoped artifact regeneration"
        )
    if action == "SCOPE_NARROW_AND_REVALIDATE_ONLY":
        return "local edge can be re-aggregated from existing artifacts without generator changes"
    if action == "REJECT_CURRENT_FORM":
        return "current refined confidence scaling form should be archived"
    if action == "FORWARD_OBSERVE_ONLY":
        return "evidence is weak and should age forward before more implementation"
    return "current family lacks enough local edge for continued confidence-scaling iteration"


def _primary_scope_opportunity(
    *,
    original_id: str,
    local_edge_label: str,
    is_reject_candidate: bool,
) -> str:
    if is_reject_candidate:
        return "current_form_reject"
    if local_edge_label not in {"LOCAL_EDGE_PRESENT", "LOCAL_EDGE_WEAK"}:
        return "none"
    if "baseline_plus_trend_structure" in original_id:
        return "trend_confirmation_high_conviction"
    if "volatility_regime" in original_id:
        return "risk_cap_or_veto_high_conviction"
    return "scope_narrowing_research_only"


def _false_cost_recommended_usage(
    source: Mapping[str, Any],
    dimension: str,
    scope_value: str,
    label: str,
) -> str:
    if label != "FALSE_COST_ACCEPTABLE":
        return "reject_scope"
    score = to_float(source.get("confidence_weighted_score"))
    candidate_id = str(source.get("refined_candidate_id") or "")
    if dimension == "direction" and scope_value in {"risk_off", "volatility_expansion"}:
        return "risk_cap_only"
    if dimension == "direction" and "trend" in scope_value:
        return "confirmation_only"
    if "volatility_regime" in candidate_id:
        return "risk_cap_only"
    if "baseline_plus_trend_structure" in candidate_id:
        return "confirmation_only"
    if score > 0.0:
        return "continue_research"
    return "continue_research"


def _risk_appetite_reject_reason(status: str, comparison: Mapping[str, Any]) -> str:
    if status == "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED":
        return (
            "TRADING-2289 classified current refined risk_appetite form as reject "
            "recommended; no owner-review candidate recommendation was emitted."
        )
    label = comparison.get("comparison_label") or "unknown"
    return f"risk_appetite current form lacks accepted 2289 reject status; comparison_label={label}"


def _output_paths(output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "local_edge_scope_review_summary.json",
        "local_edge_json": output_dir / "candidate_local_edge_matrix.json",
        "local_edge_csv": output_dir / "candidate_local_edge_matrix.csv",
        "asset_json": output_dir / "candidate_asset_scope_matrix.json",
        "asset_csv": output_dir / "candidate_asset_scope_matrix.csv",
        "horizon_json": output_dir / "candidate_horizon_scope_matrix.json",
        "horizon_csv": output_dir / "candidate_horizon_scope_matrix.csv",
        "direction_json": output_dir / "candidate_direction_scope_matrix.json",
        "direction_csv": output_dir / "candidate_direction_scope_matrix.csv",
        "high_conviction_json": output_dir / "candidate_high_conviction_scope_matrix.json",
        "high_conviction_csv": output_dir / "candidate_high_conviction_scope_matrix.csv",
        "regime_json": output_dir / "candidate_regime_scope_matrix.json",
        "regime_csv": output_dir / "candidate_regime_scope_matrix.csv",
        "false_cost_json": output_dir / "candidate_false_cost_scope_matrix.json",
        "false_cost_csv": output_dir / "candidate_false_cost_scope_matrix.csv",
        "scope_json": output_dir / "candidate_scope_narrowing_recommendation_matrix.json",
        "scope_csv": output_dir / "candidate_scope_narrowing_recommendation_matrix.csv",
        "risk_appetite_json": output_dir / "risk_appetite_reject_record.json",
        "risk_appetite_runtime_md": output_dir / "risk_appetite_reject_record.md",
        "next_task_json": output_dir / "candidate_next_task_recommendation_matrix.json",
        "decision_summary_json": output_dir / "candidate_scope_review_decision_summary.json",
        "local_edge_doc": docs_root / "refined_candidate_local_edge_scope_review.md",
        "scope_doc": docs_root / "refined_candidate_scope_narrowing_recommendation.md",
        "risk_appetite_doc": docs_root / "risk_appetite_reject_record.md",
        "next_task_doc": docs_root / "candidate_next_task_recommendation_after_2289.md",
    }


def _common_payload(*, generated_at: datetime, mode: str) -> dict[str, Any]:
    return {
        "schema_version": "refined_candidate_local_edge_scope_review.v1",
        "report_type": ARTIFACT_ROLE,
        "title": "Refined Candidate Local Edge and Scope Narrowing Review",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        **_safety_subset(),
    }


def _safety_subset() -> dict[str, Any]:
    return dict(SAFETY_FIELDS)


def _render_local_edge_doc(
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Refined Candidate Local Edge Scope Review",
        "",
        "最后更新：2026-06-30",
        "",
        (
            "TRADING-2289 已完成 refined actual-path validation；owner review "
            "candidate recommendation 全部为 false。TRADING-2290 不生成 owner "
            "review package，只做 local edge / scope narrowing review。"
        ),
        "",
        "## Summary",
        "",
        f"- status: `{summary.get('status')}`",
        (
            "- input_actual_path_records_from_2289: "
            f"`{summary.get('input_actual_path_records_from_2289')}`"
        ),
        f"- input_eligible_records_from_2289: `{summary.get('input_eligible_records_from_2289')}`",
        f"- owner_review_candidate_count: `{summary.get('owner_review_candidate_count')}`",
        f"- scope_narrowing_candidate_count: `{summary.get('scope_narrowing_candidate_count')}`",
        f"- next_task_recommendation: `{summary.get('next_task_recommendation')}`",
        "",
        "## Candidate Local Edge",
        "",
        "|refined_candidate_id|status_from_2289|alignment|high_conviction_alignment|guardrail|local_edge_label|primary_scope_opportunity|",
        "|---|---:|---:|---:|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "|{cid}|{status}|{align}|{high}|{guardrail}|{label}|{scope}|".format(
                cid=row.get("refined_candidate_id"),
                status=row.get("research_status_from_2289"),
                align=row.get("alignment_rate"),
                high=row.get("high_conviction_alignment_rate"),
                guardrail=row.get("guardrail_status"),
                label=row.get("local_edge_label"),
                scope=row.get("primary_scope_opportunity"),
            )
        )
    lines.extend(
        [
            "",
            (
                "本报告中的 local edge 只表示后续 scope-narrowed research 线索，"
                "不代表 promotion、paper-shadow、production 或 broker readiness。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_scope_recommendation_doc(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Refined Candidate Scope Narrowing Recommendation",
        "",
        "最后更新：2026-06-30",
        "",
        "|refined_candidate_id|scope_action|usage|kept_assets|kept_horizons|kept_directions|next_task|",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "|{cid}|{action}|{usage}|{assets}|{horizons}|{directions}|{next_task}|".format(
                cid=row.get("refined_candidate_id"),
                action=row.get("recommended_scope_action"),
                usage=row.get("usage_recommendation"),
                assets=",".join(row.get("kept_assets") or []),
                horizons=",".join(row.get("kept_horizons") or []),
                directions=",".join(row.get("kept_directions") or []),
                next_task=row.get("next_task_recommendation"),
            )
        )
    lines.extend(
        [
            "",
            (
                "所有 recommendation 均为 research-only；promotion / paper-shadow / "
                "production / broker 全部继续阻断。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_risk_appetite_reject_doc(record: Mapping[str, Any]) -> str:
    return (
        "# Risk Appetite Current-Form Reject Record\n\n"
        "最后更新：2026-06-30\n\n"
        f"- candidate_id: `{record.get('candidate_id')}`\n"
        f"- original_candidate_id: `{record.get('original_candidate_id')}`\n"
        f"- reject_scope: `{record.get('reject_scope')}`\n"
        f"- recommended_future_action: `{record.get('recommended_future_action')}`\n"
        f"- reject_reason: {record.get('reject_reason')}\n"
        "- risk_appetite_concept_permanently_rejected: "
        f"`{record.get('risk_appetite_concept_permanently_rejected')}`\n"
        f"- future_reopen_condition: `{record.get('future_reopen_condition')}`\n"
        "\nReject current form 只表示当前 first-layer risk_appetite generator + "
        "refined confidence scaling 路线不值得继续消耗近期研究预算；不代表 "
        "risk appetite concept 永久无效。\n\n"
        "promotion / paper-shadow / production / broker 全部继续阻断。\n"
    )


def _render_next_task_doc(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Candidate Next Task Recommendation After 2289",
        "",
        "最后更新：2026-06-30",
        "",
        "|refined_candidate_id|scope_action|next_task|rationale|",
        "|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "|{cid}|{action}|{task}|{rationale}|".format(
                cid=row.get("refined_candidate_id"),
                action=row.get("recommended_scope_action"),
                task=row.get("next_task"),
                rationale=row.get("next_task_rationale"),
            )
        )
    lines.append("")
    lines.append(
        "TRADING-2290 不输出 owner review package，不输出 promotion-ready / "
        "paper-shadow-ready / production-ready / broker-ready 状态。"
    )
    return "\n".join(lines) + "\n"


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(parse_csv_list(value))
    return tuple(str(item) for item in value if str(item))


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}
