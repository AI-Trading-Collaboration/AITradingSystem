from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
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

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "regenerated_candidate_inconclusive_diagnostics"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2286_REGENERATED_CANDIDATE_INCONCLUSIVE_DIAGNOSTICS"
STATUS = "INCONCLUSIVE_DIAGNOSTICS_READY_PROMOTION_BLOCKED"
MODE = "inconclusive_diagnostics"
ARTIFACT_ROLE = "regenerated_candidate_inconclusive_diagnostics"
DEFAULT_CANDIDATES = (
    "baseline_plus_trend_structure",
    "risk_appetite",
    "volatility_regime",
)

# Research-only diagnostic thresholds for TRADING-2286. These are not promotion,
# paper-shadow, production, or broker gates; TRADING-2287 must review them.
HIGH_CONFIDENCE_THRESHOLD = 0.65
LOW_CONFIDENCE_THRESHOLD = 0.35
NEUTRAL_DOMINANCE_THRESHOLD = 0.70
MINIMUM_DIRECTIONAL_SIGNAL_RATIO = 0.20
MINIMUM_LOCAL_SAMPLE = 100
LOCAL_WEAK_EDGE_MIN_SCORE = 0.0
LOCAL_NEGATIVE_EDGE_MAX_SCORE = 0.0
MATERIAL_FALSE_SIGNAL_COST = 0.08
FALSE_SIGNAL_RATE_THRESHOLD = 0.10
HIGH_REDUNDANCY_CORRELATION = 0.85
HIGH_REDUNDANCY_AGREEMENT = 0.80
COMPLEMENTARY_CORRELATION = 0.50
UNSTABLE_DISAGREEMENT_RATE = 0.50
DATA_QUALITY_MATERIALITY_THRESHOLD = 0.05
MINIMUM_ELIGIBLE_RATIO = 0.75
REGIME_MINIMUM_SAMPLE = 100
UPTREND_RETURN_THRESHOLD = 0.02
DOWNTREND_RETURN_THRESHOLD = -0.02
DRAWDOWN_REGIME_THRESHOLD = -0.03
HIGH_VOLATILITY_REGIME_THRESHOLD = 0.25
LOW_VOLATILITY_REGIME_THRESHOLD = 0.15

BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}


class RegeneratedCandidateInconclusiveDiagnosticsError(ValueError):
    pass


@dataclass(frozen=True)
class RegeneratedCandidateDiagnosticsInputs:
    validation_dir: Path
    generator_dir: Path
    summary: dict[str, Any]
    actual_path_rows: list[dict[str, Any]]
    outcome_rows: list[dict[str, Any]]
    scorecards: list[dict[str, Any]]
    error_rows: list[dict[str, Any]]
    data_quality_rows: list[dict[str, Any]]
    state_recommendation_rows: list[dict[str, Any]]
    generator_context_rows: list[dict[str, Any]]
    generator_artifact_context_status: str
    generator_context_warning_count: int


def run_regenerated_candidate_inconclusive_diagnostics(
    *,
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
        raise RegeneratedCandidateInconclusiveDiagnosticsError(
            "regenerated candidate inconclusive diagnostics only supports "
            "inconclusive_diagnostics mode"
        )
    candidate_ids = _normalize_list(candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    inputs = load_regenerated_candidate_inconclusive_diagnostics_inputs(
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
    actual_path_rows = [
        row
        for row in inputs.actual_path_rows
        if str(row.get("candidate_id")) in candidate_ids
        and str(row.get("target_asset")) in asset_ids
        and str(row.get("horizon")) in horizon_ids
    ]

    density_rows = build_candidate_signal_density_matrix(outcome_rows)
    confidence_rows = build_candidate_confidence_distribution_matrix(outcome_rows)
    horizon_asset_rows = build_candidate_horizon_asset_drilldown(outcome_rows)
    direction_rows = build_candidate_direction_alignment_drilldown(outcome_rows)
    false_cost_rows = build_candidate_false_signal_cost_matrix(outcome_rows)
    overlap_rows = build_candidate_signal_overlap_matrix(outcome_rows)
    data_quality_impact_rows = build_candidate_data_quality_impact_matrix(outcome_rows)
    regime_rows = build_candidate_regime_drilldown_matrix(outcome_rows)
    recommendation_rows = build_candidate_refinement_recommendation_matrix(
        candidate_ids=candidate_ids,
        density_rows=density_rows,
        horizon_asset_rows=horizon_asset_rows,
        direction_rows=direction_rows,
        false_cost_rows=false_cost_rows,
        overlap_rows=overlap_rows,
        data_quality_impact_rows=data_quality_impact_rows,
        regime_rows=regime_rows,
        scorecards=inputs.scorecards,
        state_recommendations=inputs.state_recommendation_rows,
    )

    summary = _diagnostics_summary(
        candidate_ids=candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        generated_at=generated_at,
        inputs=inputs,
        actual_path_rows=actual_path_rows,
        outcome_rows=outcome_rows,
        recommendation_rows=recommendation_rows,
        diagnostic_matrix_count=9,
    )
    utility_summary = _utility_summary(
        summary=summary,
        recommendation_rows=recommendation_rows,
    )
    common = _common_payload(generated_at=generated_at, mode=mode, summary=summary)
    paths = _artifact_paths(output_dir=output_dir, docs_root=docs_root)

    write_json(paths["summary"], {**common, "summary": summary})
    write_matrix_artifacts(
        paths["signal_density_json"], paths["signal_density_csv"], common, density_rows
    )
    write_matrix_artifacts(
        paths["confidence_distribution_json"],
        paths["confidence_distribution_csv"],
        common,
        confidence_rows,
    )
    write_matrix_artifacts(
        paths["horizon_asset_drilldown_json"],
        paths["horizon_asset_drilldown_csv"],
        common,
        horizon_asset_rows,
    )
    write_matrix_artifacts(
        paths["direction_alignment_drilldown_json"],
        paths["direction_alignment_drilldown_csv"],
        common,
        direction_rows,
    )
    write_matrix_artifacts(
        paths["false_signal_cost_json"],
        paths["false_signal_cost_csv"],
        common,
        false_cost_rows,
    )
    write_matrix_artifacts(
        paths["signal_overlap_json"],
        paths["signal_overlap_csv"],
        common,
        overlap_rows,
    )
    write_matrix_artifacts(
        paths["data_quality_impact_json"],
        paths["data_quality_impact_csv"],
        common,
        data_quality_impact_rows,
    )
    write_matrix_artifacts(
        paths["regime_drilldown_json"], paths["regime_drilldown_csv"], common, regime_rows
    )
    write_matrix_artifacts(
        paths["refinement_recommendation_json"],
        paths["refinement_recommendation_csv"],
        common,
        recommendation_rows,
    )
    write_json(paths["utility_drilldown_summary"], {**common, "summary": utility_summary})
    write_markdown(
        paths["diagnostics_report_doc"],
        _render_diagnostics_report(
            summary,
            recommendation_rows,
            density_rows=density_rows,
            horizon_asset_rows=horizon_asset_rows,
            direction_rows=direction_rows,
            false_cost_rows=false_cost_rows,
            overlap_rows=overlap_rows,
            data_quality_impact_rows=data_quality_impact_rows,
        ),
    )
    write_markdown(
        paths["utility_drilldown_doc"],
        _render_utility_drilldown_doc(summary, density_rows, horizon_asset_rows),
    )
    write_markdown(
        paths["refinement_recommendation_doc"],
        _render_refinement_recommendation_doc(
            summary,
            recommendation_rows,
            false_cost_rows=false_cost_rows,
            overlap_rows=overlap_rows,
            data_quality_impact_rows=data_quality_impact_rows,
        ),
    )

    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "utility_summary": utility_summary,
            "candidate_recommendations": recommendation_rows,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "diagnostic_matrix_count": 9,
        }
    )


def load_regenerated_candidate_inconclusive_diagnostics_inputs(
    *,
    validation_dir: Path,
    generator_dir: Path,
    candidates: Sequence[str] | str,
) -> RegeneratedCandidateDiagnosticsInputs:
    candidate_ids = _normalize_list(candidates)
    paths = {
        "summary": validation_dir / "regenerated_candidate_actual_path_validation_summary.json",
        "actual_path_matrix": validation_dir / "regenerated_candidate_actual_path_matrix.json",
        "actual_path_matrix_csv": validation_dir / "regenerated_candidate_actual_path_matrix.csv",
        "outcome_matrix": validation_dir / "candidate_prediction_outcome_matrix.json",
        "outcome_matrix_csv": validation_dir / "candidate_prediction_outcome_matrix.csv",
        "scorecard": validation_dir / "candidate_validation_scorecard.json",
        "error_seed": validation_dir / "candidate_error_attribution_seed.json",
        "data_quality": validation_dir / "candidate_data_quality_report.json",
        "state_recommendations": validation_dir / "candidate_state_recommendation_matrix.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise RegeneratedCandidateInconclusiveDiagnosticsError(
            f"missing regenerated candidate actual-path validation output(s): {missing}"
        )

    summary_payload = _read_json(paths["summary"])
    actual_path_payload = _read_json(paths["actual_path_matrix"])
    outcome_payload = _read_json(paths["outcome_matrix"])
    scorecard_payload = _read_json(paths["scorecard"])
    error_payload = _read_json(paths["error_seed"])
    data_quality_payload = _read_json(paths["data_quality"])
    state_payload = _read_json(paths["state_recommendations"])

    payloads = {
        "summary": summary_payload,
        "actual_path_matrix": actual_path_payload,
        "outcome_matrix": outcome_payload,
        "scorecard": scorecard_payload,
        "error_seed": error_payload,
        "data_quality": data_quality_payload,
        "state_recommendations": state_payload,
    }
    for name, payload in payloads.items():
        _assert_safe_payload(name, payload, require_safety_fields=True)
        _assert_no_banned_recommendations(name, payload)

    actual_path_rows = _rows_from_payload(actual_path_payload, "rows")
    outcome_rows = _rows_from_payload(outcome_payload, "rows")
    scorecards = _rows_from_payload(scorecard_payload, "candidate_scorecards")
    error_rows = _rows_from_payload(error_payload, "error_rows")
    data_quality_rows = _rows_from_payload(data_quality_payload, "candidate_rows")
    state_recommendation_rows = _rows_from_payload(state_payload, "candidate_rows")
    for name, rows in (
        ("actual_path_matrix", actual_path_rows),
        ("outcome_matrix", outcome_rows),
        ("scorecard", scorecards),
        ("error_seed", error_rows),
        ("data_quality", data_quality_rows),
        ("state_recommendations", state_recommendation_rows),
    ):
        for index, row in enumerate(rows):
            _assert_safe_payload(f"{name}[{index}]", row, require_safety_fields=False)
            _assert_no_banned_recommendations(f"{name}[{index}]", row)

    generator_rows = _load_generator_context_rows(generator_dir, candidate_ids)
    warning_count = sum(1 for row in generator_rows if bool(row.get("data_quality_warning")))
    context_status = (
        "complete"
        if generator_rows
        and all(
            row.get("generator_artifact_context_status") == "complete" for row in generator_rows
        )
        else "partial"
    )
    return RegeneratedCandidateDiagnosticsInputs(
        validation_dir=validation_dir,
        generator_dir=generator_dir,
        summary=summary_payload,
        actual_path_rows=actual_path_rows,
        outcome_rows=outcome_rows,
        scorecards=scorecards,
        error_rows=error_rows,
        data_quality_rows=data_quality_rows,
        state_recommendation_rows=state_recommendation_rows,
        generator_context_rows=generator_rows,
        generator_artifact_context_status=context_status,
        generator_context_warning_count=warning_count,
    )


def build_candidate_signal_density_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
    *,
    high_confidence_threshold: float = HIGH_CONFIDENCE_THRESHOLD,
    low_confidence_threshold: float = LOW_CONFIDENCE_THRESHOLD,
    neutral_dominance_threshold: float = NEUTRAL_DOMINANCE_THRESHOLD,
    minimum_directional_signal_ratio: float = MINIMUM_DIRECTIONAL_SIGNAL_RATIO,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, group in _group_rows(
        outcome_rows, ("candidate_id", "target_asset", "horizon")
    ).items():
        candidate_id, target_asset, horizon = key
        directions = Counter(str(row.get("signal_direction") or "neutral") for row in group)
        record_count = len(group)
        eligible_count = sum(1 for row in group if bool(row.get("validation_eligible")))
        high_count = sum(
            1
            for row in group
            if to_float(row.get("signal_confidence")) >= high_confidence_threshold
        )
        low_count = sum(
            1 for row in group if to_float(row.get("signal_confidence")) <= low_confidence_threshold
        )
        neutral_count = directions["neutral"]
        directional_count = record_count - neutral_count
        neutral_ratio = _ratio(neutral_count, record_count)
        high_ratio = _ratio(high_count, record_count)
        directional_ratio = _ratio(directional_count, record_count)
        labels: list[str] = []
        if neutral_ratio > neutral_dominance_threshold:
            labels.append("POSSIBLE_OVER_NEUTRALIZATION")
        if high_ratio < minimum_directional_signal_ratio:
            labels.append("LOW_CONVICTION_SIGNAL_DESIGN")
        if directional_ratio < minimum_directional_signal_ratio:
            labels.append("LOW_DIRECTIONAL_SIGNAL_DENSITY")
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "record_count": record_count,
                    "eligible_record_count": eligible_count,
                    "neutral_count": neutral_count,
                    "risk_on_count": directions["risk_on"],
                    "risk_off_count": directions["risk_off"],
                    "trend_confirming_count": directions["trend_confirming"],
                    "trend_weakening_count": directions["trend_weakening"],
                    "volatility_expansion_count": directions["volatility_expansion"],
                    "volatility_compression_count": directions["volatility_compression"],
                    "high_confidence_record_count": high_count,
                    "low_confidence_record_count": low_count,
                    "neutral_ratio": round_float(neutral_ratio),
                    "high_confidence_ratio": round_float(high_ratio),
                    "directional_signal_ratio": round_float(directional_ratio),
                    "diagnostic_labels": labels,
                    "diagnostic_label": labels[0] if labels else "SIGNAL_DENSITY_ACCEPTABLE",
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id", "target_asset", "horizon"))


def build_candidate_confidence_distribution_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, group in _group_rows(
        outcome_rows, ("candidate_id", "target_asset", "horizon")
    ).items():
        candidate_id, target_asset, horizon = key
        values = [to_float(row.get("signal_confidence")) for row in group]
        low = sum(1 for value in values if value <= LOW_CONFIDENCE_THRESHOLD)
        high = sum(1 for value in values if value >= HIGH_CONFIDENCE_THRESHOLD)
        mid = len(values) - low - high
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "record_count": len(values),
                    "low_confidence_count": low,
                    "medium_confidence_count": mid,
                    "high_confidence_count": high,
                    "average_confidence": round_float(_mean(values)),
                    "min_confidence": round_float(min(values)) if values else 0.0,
                    "max_confidence": round_float(max(values)) if values else 0.0,
                    "low_confidence_ratio": round_float(_ratio(low, len(values))),
                    "high_confidence_ratio": round_float(_ratio(high, len(values))),
                    "diagnostic_label": (
                        "LOW_CONVICTION_SIGNAL_DESIGN"
                        if _ratio(high, len(values)) < MINIMUM_DIRECTIONAL_SIGNAL_RATIO
                        else "CONFIDENCE_DISTRIBUTION_ACCEPTABLE"
                    ),
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id", "target_asset", "horizon"))


def build_candidate_horizon_asset_drilldown(
    outcome_rows: Sequence[Mapping[str, Any]],
    *,
    minimum_local_sample: int = MINIMUM_LOCAL_SAMPLE,
) -> list[dict[str, Any]]:
    groups = _group_rows(
        outcome_rows,
        ("candidate_id", "target_asset", "horizon", "signal_direction"),
    )
    rows: list[dict[str, Any]] = []
    for key, group in groups.items():
        candidate_id, target_asset, horizon, direction = key
        eligible = [row for row in group if bool(row.get("validation_eligible"))]
        false_risk_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
        false_risk_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
        confidence_weighted = _confidence_weighted_score(eligible)
        false_cost = _error_cost(false_risk_on) + _error_cost(false_risk_off)
        label = _local_edge_label(
            eligible_count=len(eligible),
            confidence_weighted=confidence_weighted,
            false_signal_cost=false_cost,
            minimum_local_sample=minimum_local_sample,
        )
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "signal_direction": direction,
                    "record_count": len(group),
                    "eligible_record_count": len(eligible),
                    "alignment_rate": round_float(_alignment_rate(eligible)),
                    "weighted_alignment_score": round_float(_weighted_score(eligible)),
                    "confidence_weighted_alignment_score": round_float(confidence_weighted),
                    "false_risk_on_count": len(false_risk_on),
                    "false_risk_off_count": len(false_risk_off),
                    "false_risk_on_cost": round_float(_error_cost(false_risk_on)),
                    "false_risk_off_cost": round_float(_error_cost(false_risk_off)),
                    "average_forward_return": round_float(
                        _mean_optional(eligible, "actual_forward_return")
                    ),
                    "average_max_drawdown": round_float(
                        _mean_optional(eligible, "actual_max_drawdown")
                    ),
                    "average_realized_volatility": round_float(
                        _mean_optional(eligible, "actual_realized_volatility")
                    ),
                    "diagnostic_label": label,
                    "diagnostic_labels": [label],
                    **_safety_fields(),
                }
            )
        )
    _apply_mixed_scope_labels(rows)
    return _sorted_rows(rows, ("candidate_id", "target_asset", "horizon", "signal_direction"))


def build_candidate_direction_alignment_drilldown(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, group in _group_rows(outcome_rows, ("candidate_id", "signal_direction")).items():
        candidate_id, direction = key
        eligible = [row for row in group if bool(row.get("validation_eligible"))]
        false_risk_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
        false_risk_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
        partial = [row for row in eligible if row.get("alignment_label") == "partial_alignment"]
        negative = [row for row in eligible if to_float(row.get("alignment_score")) < 0.0]
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "direction": direction,
                    "record_count": len(group),
                    "eligible_record_count": len(eligible),
                    "alignment_rate": round_float(_alignment_rate(eligible)),
                    "partial_alignment_rate": round_float(_ratio(len(partial), len(eligible))),
                    "negative_alignment_rate": round_float(_ratio(len(negative), len(eligible))),
                    "false_risk_on_rate": round_float(_ratio(len(false_risk_on), len(eligible))),
                    "false_risk_off_rate": round_float(_ratio(len(false_risk_off), len(eligible))),
                    "average_forward_return": round_float(
                        _mean_optional(eligible, "actual_forward_return")
                    ),
                    "average_max_drawdown": round_float(
                        _mean_optional(eligible, "actual_max_drawdown")
                    ),
                    "average_volatility": round_float(
                        _mean_optional(eligible, "actual_realized_volatility")
                    ),
                    "diagnostic_label": _direction_label(direction, eligible),
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id", "direction"))


def build_candidate_false_signal_cost_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate_id, group in _group_rows(outcome_rows, ("candidate_id",)).items():
        candidate_rows = list(group)
        eligible = [row for row in candidate_rows if bool(row.get("validation_eligible"))]
        false_risk_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
        false_risk_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
        false_on_cost = _error_cost(false_risk_on)
        false_off_cost = _error_cost(false_risk_off)
        false_on_rate = _ratio(len(false_risk_on), len(eligible))
        false_off_rate = _ratio(len(false_risk_off), len(eligible))
        missed_upside = [
            row for row in eligible if row.get("error_type") in {"false_risk_off", "missed_upside"}
        ]
        missed_downside = [
            row for row in eligible if row.get("error_type") in {"false_risk_on", "missed_downside"}
        ]
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id[0],
                    "record_count": len(candidate_rows),
                    "eligible_record_count": len(eligible),
                    "false_risk_on_count": len(false_risk_on),
                    "false_risk_on_rate": round_float(false_on_rate),
                    "false_risk_on_cost_total": round_float(false_on_cost),
                    "false_risk_on_cost_average": round_float(
                        false_on_cost / len(false_risk_on) if false_risk_on else 0.0
                    ),
                    "false_risk_off_count": len(false_risk_off),
                    "false_risk_off_rate": round_float(false_off_rate),
                    "false_risk_off_cost_total": round_float(false_off_cost),
                    "false_risk_off_cost_average": round_float(
                        false_off_cost / len(false_risk_off) if false_risk_off else 0.0
                    ),
                    "missed_upside_count": len(missed_upside),
                    "missed_downside_count": len(missed_downside),
                    "cost_asymmetry": round_float(false_off_cost - false_on_cost),
                    "diagnostic_label": _false_cost_label(
                        false_on_cost=false_on_cost,
                        false_off_cost=false_off_cost,
                        false_on_rate=false_on_rate,
                        false_off_rate=false_off_rate,
                    ),
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id",))


def build_candidate_signal_overlap_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    compressed = _compress_candidate_key_rows(outcome_rows)
    by_candidate = _group_rows(compressed, ("candidate_id",))
    rows: list[dict[str, Any]] = []
    candidate_ids = sorted(str(key[0]) for key in by_candidate)
    for left_id, right_id in combinations(candidate_ids, 2):
        left = {_overlap_key(row): row for row in by_candidate[(left_id,)]}
        right = {_overlap_key(row): row for row in by_candidate[(right_id,)]}
        common_keys = sorted(set(left) & set(right))
        left_values = [to_float(left[key].get("signal_value")) for key in common_keys]
        right_values = [to_float(right[key].get("signal_value")) for key in common_keys]
        left_conf = [to_float(left[key].get("signal_confidence")) for key in common_keys]
        right_conf = [to_float(right[key].get("signal_confidence")) for key in common_keys]
        agreement = sum(
            1
            for key in common_keys
            if left[key].get("signal_direction") == right[key].get("signal_direction")
        )
        same_neutral = sum(
            1
            for key in common_keys
            if left[key].get("signal_direction") == right[key].get("signal_direction") == "neutral"
        )
        same_risk_on = sum(
            1
            for key in common_keys
            if left[key].get("signal_direction") == right[key].get("signal_direction")
            and left[key].get("signal_direction") in {"risk_on", "trend_confirming"}
        )
        same_risk_off = sum(
            1
            for key in common_keys
            if left[key].get("signal_direction") == right[key].get("signal_direction")
            and left[key].get("signal_direction")
            in {"risk_off", "trend_weakening", "volatility_expansion"}
        )
        disagreement_keys = [
            key
            for key in common_keys
            if left[key].get("signal_direction") != right[key].get("signal_direction")
        ]
        advantage = _disagreement_outcome_advantage(left, right, disagreement_keys)
        value_corr = _correlation(left_values, right_values)
        direction_agreement_rate = _ratio(agreement, len(common_keys))
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id_left": left_id,
                    "candidate_id_right": right_id,
                    "aligned_record_count": len(common_keys),
                    "signal_value_correlation": round_float(value_corr),
                    "signal_direction_agreement_rate": round_float(direction_agreement_rate),
                    "confidence_correlation": round_float(_correlation(left_conf, right_conf)),
                    "same_neutral_rate": round_float(_ratio(same_neutral, len(common_keys))),
                    "same_risk_on_rate": round_float(_ratio(same_risk_on, len(common_keys))),
                    "same_risk_off_rate": round_float(_ratio(same_risk_off, len(common_keys))),
                    "disagreement_count": len(disagreement_keys),
                    "disagreement_outcome_advantage": round_float(advantage),
                    "diagnostic_label": _overlap_label(
                        signal_value_correlation=value_corr,
                        direction_agreement_rate=direction_agreement_rate,
                        disagreement_rate=_ratio(len(disagreement_keys), len(common_keys)),
                        disagreement_outcome_advantage=advantage,
                    ),
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id_left", "candidate_id_right"))


def build_candidate_data_quality_impact_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate_id, group in _group_rows(outcome_rows, ("candidate_id",)).items():
        candidate_rows = list(group)
        warnings = [row for row in candidate_rows if bool(row.get("data_quality_warning"))]
        eligible = [row for row in candidate_rows if bool(row.get("validation_eligible"))]
        no_warning = [
            row
            for row in candidate_rows
            if bool(row.get("validation_eligible")) and not bool(row.get("data_quality_warning"))
        ]
        status_counts = Counter(str(row.get("actual_path_status")) for row in candidate_rows)
        score_including = _confidence_weighted_score(candidate_rows)
        score_excluding = _confidence_weighted_score(no_warning)
        delta = score_including - score_excluding
        eligible_ratio = _ratio(len(eligible), len(candidate_rows))
        label = _data_quality_impact_label(delta=delta, eligible_ratio=eligible_ratio)
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id[0],
                    "data_quality_status": ("PASS_WITH_WARNINGS" if warnings else "PASS"),
                    "warning_count": len(warnings),
                    "warning_rate": round_float(_ratio(len(warnings), len(candidate_rows))),
                    "incomplete_future_window_count": status_counts["incomplete_future_window"],
                    "missing_decision_price_count": status_counts["missing_decision_price"],
                    "partial_price_coverage_count": status_counts["partial_price_coverage"],
                    "low_coverage_record_count": sum(
                        1
                        for row in candidate_rows
                        if row.get("actual_path_status") == "partial_price_coverage"
                        and not bool(row.get("validation_eligible"))
                    ),
                    "eligible_ratio": round_float(eligible_ratio),
                    "score_excluding_warning_records": round_float(score_excluding),
                    "score_including_warning_records": round_float(score_including),
                    "score_delta_due_to_warnings": round_float(delta),
                    "diagnostic_label": label,
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id",))


def build_candidate_regime_drilldown_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
    *,
    minimum_regime_sample: int = REGIME_MINIMUM_SAMPLE,
) -> list[dict[str, Any]]:
    enriched = [
        {**dict(row), "regime_label": _diagnostic_regime_label(row)} for row in outcome_rows
    ]
    rows: list[dict[str, Any]] = []
    for key, group in _group_rows(enriched, ("candidate_id", "regime_label")).items():
        candidate_id, regime_label = key
        eligible = [row for row in group if bool(row.get("validation_eligible"))]
        false_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
        false_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
        score = _confidence_weighted_score(eligible)
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "regime_label": regime_label,
                    "record_count": len(group),
                    "eligible_record_count": len(eligible),
                    "alignment_rate": round_float(_alignment_rate(eligible)),
                    "weighted_alignment_score": round_float(_weighted_score(eligible)),
                    "confidence_weighted_alignment_score": round_float(score),
                    "false_risk_on_rate": round_float(_ratio(len(false_on), len(eligible))),
                    "false_risk_off_rate": round_float(_ratio(len(false_off), len(eligible))),
                    "average_forward_return": round_float(
                        _mean_optional(eligible, "actual_forward_return")
                    ),
                    "average_max_drawdown": round_float(
                        _mean_optional(eligible, "actual_max_drawdown")
                    ),
                    "diagnostic_label": _regime_label(
                        eligible_count=len(eligible),
                        score=score,
                        minimum_regime_sample=minimum_regime_sample,
                    ),
                    **_safety_fields(),
                }
            )
        )
    return _sorted_rows(rows, ("candidate_id", "regime_label"))


def build_candidate_refinement_recommendation_matrix(
    *,
    candidate_ids: Sequence[str] | str,
    density_rows: Sequence[Mapping[str, Any]],
    horizon_asset_rows: Sequence[Mapping[str, Any]],
    direction_rows: Sequence[Mapping[str, Any]],
    false_cost_rows: Sequence[Mapping[str, Any]],
    overlap_rows: Sequence[Mapping[str, Any]],
    data_quality_impact_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]] = (),
    state_recommendations: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    candidate_tuple = _normalize_list(candidate_ids)
    score_by_candidate = {str(row.get("candidate_id")): row for row in scorecards}
    state_by_candidate = {str(row.get("candidate_id")): row for row in state_recommendations}
    rows: list[dict[str, Any]] = []
    for candidate_id in candidate_tuple:
        reasons = _candidate_reasons(
            candidate_id=candidate_id,
            density_rows=density_rows,
            horizon_asset_rows=horizon_asset_rows,
            direction_rows=direction_rows,
            false_cost_rows=false_cost_rows,
            overlap_rows=overlap_rows,
            data_quality_impact_rows=data_quality_impact_rows,
            regime_rows=regime_rows,
            scorecard=score_by_candidate.get(candidate_id, {}),
        )
        primary = reasons[0] if reasons else "NO_MEASURABLE_EDGE"
        action = _recommended_next_action(primary)
        priority = _refinement_priority(primary, action)
        _assert_allowed_recommendation(action)
        current_state = str(
            state_by_candidate.get(candidate_id, {}).get("recommended_research_status")
            or score_by_candidate.get(candidate_id, {}).get("recommended_research_status")
            or "ACTUAL_PATH_VALIDATED_INCONCLUSIVE"
        )
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "current_state": current_state,
                    "diagnostic_summary": _diagnostic_summary_text(primary, action),
                    "primary_inconclusive_reason": primary,
                    "secondary_inconclusive_reasons": reasons[1:],
                    "recommended_next_action": action,
                    "refinement_priority": priority,
                    "owner_review_required": False,
                    "paper_shadow_recommendation_allowed": False,
                    "production_recommendation_allowed": False,
                    **_safety_fields(),
                    **trading_2281_boundary_fields(),
                }
            )
        )
    return rows


def _load_generator_context_rows(
    generator_dir: Path,
    candidate_ids: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        candidate_dir = generator_dir / candidate_id
        paths = {
            "signal_spec": candidate_dir / "candidate_signal_spec.json",
            "prediction_artifact": candidate_dir / "candidate_prediction_artifact.json",
            "generation_summary": candidate_dir / "generation_summary.json",
            "validation_summary": candidate_dir / "validation_summary.json",
        }
        existing = {key: path for key, path in paths.items() if path.exists()}
        missing = [key for key, path in paths.items() if not path.exists()]
        status = "complete" if not missing else "partial"
        warning = bool(missing)
        signal_names: list[str] = []
        if "signal_spec" in existing:
            spec = _read_json(existing["signal_spec"])
            _assert_safe_payload(f"{candidate_id}.signal_spec", spec, require_safety_fields=False)
            signals = spec.get("signals")
            if isinstance(signals, list):
                signal_names = [
                    str(item.get("signal_name")) for item in signals if isinstance(item, Mapping)
                ]
        if "prediction_artifact" in existing:
            artifact = _read_json(existing["prediction_artifact"])
            _assert_safe_payload(
                f"{candidate_id}.prediction_artifact",
                artifact,
                require_safety_fields=False,
            )
            for index, record in enumerate(artifact.get("prediction_records", [])):
                if isinstance(record, Mapping):
                    _assert_safe_payload(
                        f"{candidate_id}.prediction_artifact.records[{index}]",
                        record,
                        require_safety_fields=False,
                    )
        rows.append(
            clean_for_yaml(
                {
                    "candidate_id": candidate_id,
                    "generator_artifact_context_status": status,
                    "data_quality_warning": warning,
                    "missing_generator_context_artifacts": missing,
                    "signal_names": signal_names,
                    "candidate_dir": str(candidate_dir),
                    **_safety_fields(),
                }
            )
        )
    return rows


def _diagnostics_summary(
    *,
    candidate_ids: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
    inputs: RegeneratedCandidateDiagnosticsInputs,
    actual_path_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    recommendation_rows: Sequence[Mapping[str, Any]],
    diagnostic_matrix_count: int,
) -> dict[str, Any]:
    eligible_count = sum(1 for row in outcome_rows if bool(row.get("validation_eligible")))
    recommendation_counts = Counter(
        str(row.get("recommended_next_action")) for row in recommendation_rows
    )
    reason_counts = Counter(
        str(row.get("primary_inconclusive_reason")) for row in recommendation_rows
    )
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "validation_dir": str(inputs.validation_dir),
            "generator_dir": str(inputs.generator_dir),
            "candidate_count": len(candidate_ids),
            "candidate_ids": list(candidate_ids),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "input_actual_path_record_count": len(actual_path_rows),
            "input_eligible_record_count": eligible_count,
            "input_prediction_outcome_record_count": len(outcome_rows),
            "diagnostic_matrix_count": diagnostic_matrix_count,
            "candidate_recommendation_counts": dict(recommendation_counts),
            "primary_inconclusive_reason_counts": dict(reason_counts),
            "generator_artifact_context_status": inputs.generator_artifact_context_status,
            "generator_context_warning_count": inputs.generator_context_warning_count,
            "source_validation_status": _summary_status(inputs.summary),
            "artifact_role": ARTIFACT_ROLE,
            "next_task_recommendation": _next_task_recommendation(recommendation_rows),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "owner_review_required": False,
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            **trading_2281_boundary_fields(),
        }
    )


def _utility_summary(
    *,
    summary: Mapping[str, Any],
    recommendation_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "status": STATUS,
            "candidate_count": summary.get("candidate_count"),
            "input_actual_path_record_count": summary.get("input_actual_path_record_count"),
            "input_eligible_record_count": summary.get("input_eligible_record_count"),
            "diagnostic_matrix_count": summary.get("diagnostic_matrix_count"),
            "candidate_recommendations": [
                {
                    "candidate_id": row.get("candidate_id"),
                    "primary_inconclusive_reason": row.get("primary_inconclusive_reason"),
                    "recommended_next_action": row.get("recommended_next_action"),
                    "refinement_priority": row.get("refinement_priority"),
                }
                for row in recommendation_rows
            ],
            "next_task_recommendation": summary.get("next_task_recommendation"),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            **trading_2281_boundary_fields(),
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    mode: str,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "regenerated_candidate_inconclusive_diagnostics.v1",
        "report_type": "regenerated_candidate_inconclusive_diagnostics",
        "artifact_role": ARTIFACT_ROLE,
        "title": "Regenerated Candidate Inconclusive Diagnostics",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "research_only": True,
        "summary_status": summary.get("status"),
        "owner_review_required": False,
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
        **_safety_fields(),
        **trading_2281_boundary_fields(),
    }


def _artifact_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "inconclusive_diagnostics_summary.json",
        "signal_density_json": output_dir / "candidate_signal_density_matrix.json",
        "signal_density_csv": output_dir / "candidate_signal_density_matrix.csv",
        "confidence_distribution_json": output_dir
        / "candidate_confidence_distribution_matrix.json",
        "confidence_distribution_csv": output_dir / "candidate_confidence_distribution_matrix.csv",
        "horizon_asset_drilldown_json": output_dir / "candidate_horizon_asset_drilldown.json",
        "horizon_asset_drilldown_csv": output_dir / "candidate_horizon_asset_drilldown.csv",
        "direction_alignment_drilldown_json": (
            output_dir / "candidate_direction_alignment_drilldown.json"
        ),
        "direction_alignment_drilldown_csv": (
            output_dir / "candidate_direction_alignment_drilldown.csv"
        ),
        "false_signal_cost_json": output_dir / "candidate_false_signal_cost_matrix.json",
        "false_signal_cost_csv": output_dir / "candidate_false_signal_cost_matrix.csv",
        "signal_overlap_json": output_dir / "candidate_signal_overlap_matrix.json",
        "signal_overlap_csv": output_dir / "candidate_signal_overlap_matrix.csv",
        "data_quality_impact_json": output_dir / "candidate_data_quality_impact_matrix.json",
        "data_quality_impact_csv": output_dir / "candidate_data_quality_impact_matrix.csv",
        "regime_drilldown_json": output_dir / "candidate_regime_drilldown_matrix.json",
        "regime_drilldown_csv": output_dir / "candidate_regime_drilldown_matrix.csv",
        "refinement_recommendation_json": (
            output_dir / "candidate_refinement_recommendation_matrix.json"
        ),
        "refinement_recommendation_csv": (
            output_dir / "candidate_refinement_recommendation_matrix.csv"
        ),
        "utility_drilldown_summary": output_dir / "candidate_utility_drilldown_summary.json",
        "diagnostics_report_doc": (
            docs_root / "regenerated_candidate_inconclusive_diagnostics_report.md"
        ),
        "utility_drilldown_doc": docs_root / "regenerated_candidate_signal_utility_drilldown.md",
        "refinement_recommendation_doc": (
            docs_root / "regenerated_candidate_refinement_recommendation.md"
        ),
    }


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RegeneratedCandidateInconclusiveDiagnosticsError(f"{path}: expected JSON object")
    return payload


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    rows = payload.get(key)
    if not isinstance(rows, list):
        raise RegeneratedCandidateInconclusiveDiagnosticsError(f"payload missing list field: {key}")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _assert_safe_payload(
    scope: str, payload: Mapping[str, Any], *, require_safety_fields: bool
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
        raise RegeneratedCandidateInconclusiveDiagnosticsError("; ".join(errors))


def _assert_no_banned_recommendations(scope: str, payload: Mapping[str, Any]) -> None:
    for field in (
        "recommended_research_status",
        "recommended_next_action",
        "primary_inconclusive_reason",
        "current_state",
    ):
        value = str(payload.get(field) or "")
        if value in BANNED_RECOMMENDATIONS:
            raise RegeneratedCandidateInconclusiveDiagnosticsError(
                f"{scope}: banned recommendation emitted: {value}"
            )


def _assert_allowed_recommendation(value: str) -> None:
    if value in BANNED_RECOMMENDATIONS:
        raise RegeneratedCandidateInconclusiveDiagnosticsError(
            f"banned recommendation emitted: {value}"
        )


def _group_rows(
    rows: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row.get(key) or "") for key in keys)].append(dict(row))
    return dict(grouped)


def _sorted_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows], key=lambda row: tuple(str(row.get(key)) for key in keys)
    )


def _alignment_rate(rows: Sequence[Mapping[str, Any]]) -> float:
    return _ratio(sum(1 for row in rows if to_float(row.get("alignment_score")) > 0.0), len(rows))


def _weighted_score(rows: Sequence[Mapping[str, Any]]) -> float:
    return _mean([to_float(row.get("alignment_score")) for row in rows])


def _confidence_weighted_score(rows: Sequence[Mapping[str, Any]]) -> float:
    weights = [max(to_float(row.get("signal_confidence")), 0.0) for row in rows]
    scores = [to_float(row.get("alignment_score")) for row in rows]
    denominator = sum(weights)
    if denominator <= 0.0:
        return 0.0
    return sum(score * weight for score, weight in zip(scores, weights, strict=True)) / denominator


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _mean_optional(rows: Sequence[Mapping[str, Any]], key: str) -> float:
    values = [_optional_float(row.get(key)) for row in rows]
    clean_values = [value for value in values if value is not None]
    return _mean(clean_values)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _error_cost(rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        abs(to_float(row.get("actual_forward_return")))
        + abs(to_float(row.get("actual_max_drawdown")))
        for row in rows
    )


def _local_edge_label(
    *,
    eligible_count: int,
    confidence_weighted: float,
    false_signal_cost: float,
    minimum_local_sample: int,
) -> str:
    if eligible_count < minimum_local_sample:
        return "INSUFFICIENT_LOCAL_SAMPLE"
    if (
        confidence_weighted > LOCAL_WEAK_EDGE_MIN_SCORE
        and false_signal_cost <= MATERIAL_FALSE_SIGNAL_COST * eligible_count
    ):
        return "LOCAL_WEAK_EDGE"
    if (
        confidence_weighted < LOCAL_NEGATIVE_EDGE_MAX_SCORE
        and false_signal_cost > MATERIAL_FALSE_SIGNAL_COST * max(eligible_count, 1)
    ):
        return "LOCAL_NEGATIVE_EDGE"
    if confidence_weighted < LOCAL_NEGATIVE_EDGE_MAX_SCORE:
        return "LOCAL_NEGATIVE_EDGE"
    return "MIXED_BY_HORIZON"


def _apply_mixed_scope_labels(rows: list[dict[str, Any]]) -> None:
    score_by_candidate_asset: dict[tuple[str, str], list[float]] = defaultdict(list)
    score_by_candidate_horizon: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        score = to_float(row.get("confidence_weighted_alignment_score"))
        score_by_candidate_asset[
            (str(row.get("candidate_id")), str(row.get("target_asset")))
        ].append(score)
        score_by_candidate_horizon[(str(row.get("candidate_id")), str(row.get("horizon")))].append(
            score
        )
    horizon_mixed = {
        key
        for key, values in score_by_candidate_asset.items()
        if values and min(values) < 0.0 < max(values)
    }
    asset_mixed = {
        key
        for key, values in score_by_candidate_horizon.items()
        if values and min(values) < 0.0 < max(values)
    }
    for row in rows:
        labels = list(row.get("diagnostic_labels") or [])
        if (str(row.get("candidate_id")), str(row.get("target_asset"))) in horizon_mixed:
            labels.append("MIXED_BY_HORIZON")
        if (str(row.get("candidate_id")), str(row.get("horizon"))) in asset_mixed:
            labels.append("MIXED_BY_ASSET")
        row["diagnostic_labels"] = sorted(set(labels))


def _direction_label(direction: str, rows: Sequence[Mapping[str, Any]]) -> str:
    false_on_rate = _ratio(
        sum(1 for row in rows if row.get("error_type") == "false_risk_on"),
        len(rows),
    )
    false_off_rate = _ratio(
        sum(1 for row in rows if row.get("error_type") == "false_risk_off"),
        len(rows),
    )
    negative_rate = _ratio(
        sum(1 for row in rows if to_float(row.get("alignment_score")) < 0.0),
        len(rows),
    )
    if (
        direction in {"risk_on", "trend_confirming"}
        and false_on_rate >= FALSE_SIGNAL_RATE_THRESHOLD
    ):
        return "RISK_ON_DRAWDOWN_STRESS_PRONE"
    if (
        direction in {"risk_off", "trend_weakening", "volatility_expansion"}
        and false_off_rate >= FALSE_SIGNAL_RATE_THRESHOLD
    ):
        return "RISK_OFF_MISSES_UPSIDE"
    if direction == "neutral" and negative_rate >= FALSE_SIGNAL_RATE_THRESHOLD:
        return "NEUTRAL_OVER_COVERS_TREND_WINDOWS"
    if direction == "volatility_compression" and negative_rate >= FALSE_SIGNAL_RATE_THRESHOLD:
        return "VOLATILITY_COMPRESSION_MISCLASSIFIED"
    return "DIRECTION_ALIGNMENT_MIXED"


def _false_cost_label(
    *,
    false_on_cost: float,
    false_off_cost: float,
    false_on_rate: float,
    false_off_rate: float,
) -> str:
    on_high = (
        false_on_cost > MATERIAL_FALSE_SIGNAL_COST and false_on_rate >= FALSE_SIGNAL_RATE_THRESHOLD
    )
    off_high = (
        false_off_cost > MATERIAL_FALSE_SIGNAL_COST
        and false_off_rate >= FALSE_SIGNAL_RATE_THRESHOLD
    )
    if on_high and off_high:
        return "BOTH_FALSE_SIGNAL_COSTS_HIGH"
    if off_high:
        return "FALSE_RISK_OFF_COST_TOO_HIGH"
    if on_high:
        return "FALSE_RISK_ON_COST_TOO_HIGH"
    return "FALSE_SIGNAL_COST_NOT_DOMINANT"


def _compress_candidate_key_rows(outcome_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups = _group_rows(
        outcome_rows,
        ("candidate_id", "decision_timestamp", "target_asset", "horizon"),
    )
    compressed: list[dict[str, Any]] = []
    for key, rows in groups.items():
        candidate_id, decision_timestamp, target_asset, horizon = key
        direction_counter = Counter(str(row.get("signal_direction") or "neutral") for row in rows)
        compressed.append(
            {
                "candidate_id": candidate_id,
                "decision_timestamp": decision_timestamp,
                "target_asset": target_asset,
                "horizon": horizon,
                "signal_direction": direction_counter.most_common(1)[0][0],
                "signal_value": _mean([to_float(row.get("signal_value")) for row in rows]),
                "signal_confidence": _mean(
                    [to_float(row.get("signal_confidence")) for row in rows]
                ),
                "alignment_score": _mean([to_float(row.get("alignment_score")) for row in rows]),
            }
        )
    return compressed


def _overlap_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("decision_timestamp") or ""),
        str(row.get("target_asset") or ""),
        str(row.get("horizon") or ""),
    )


def _disagreement_outcome_advantage(
    left: Mapping[tuple[str, str, str], Mapping[str, Any]],
    right: Mapping[tuple[str, str, str], Mapping[str, Any]],
    keys: Sequence[tuple[str, str, str]],
) -> float:
    if not keys:
        return 0.0
    return _mean(
        [
            to_float(left[key].get("alignment_score")) - to_float(right[key].get("alignment_score"))
            for key in keys
        ]
    )


def _correlation(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = _mean(left)
    right_mean = _mean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    left_var = sum((x - left_mean) ** 2 for x in left)
    right_var = sum((y - right_mean) ** 2 for y in right)
    denominator = math.sqrt(left_var * right_var)
    return numerator / denominator if denominator else 0.0


def _overlap_label(
    *,
    signal_value_correlation: float,
    direction_agreement_rate: float,
    disagreement_rate: float,
    disagreement_outcome_advantage: float,
) -> str:
    if (
        signal_value_correlation >= HIGH_REDUNDANCY_CORRELATION
        and direction_agreement_rate >= HIGH_REDUNDANCY_AGREEMENT
    ):
        return "HIGHLY_REDUNDANT"
    if (
        signal_value_correlation <= COMPLEMENTARY_CORRELATION
        and abs(disagreement_outcome_advantage) > 0.0
    ):
        return "COMPLEMENTARY"
    if (
        disagreement_rate >= UNSTABLE_DISAGREEMENT_RATE
        and abs(disagreement_outcome_advantage) < 0.05
    ):
        return "UNSTABLE_DISAGREEMENT"
    return "PARTIALLY_REDUNDANT"


def _data_quality_impact_label(*, delta: float, eligible_ratio: float) -> str:
    if eligible_ratio < MINIMUM_ELIGIBLE_RATIO:
        return "DATA_QUALITY_BLOCKS_CONCLUSION"
    if abs(delta) > DATA_QUALITY_MATERIALITY_THRESHOLD:
        return "DATA_QUALITY_MAY_AFFECT_SCORE"
    return "DATA_QUALITY_NOT_MATERIAL"


def _diagnostic_regime_label(row: Mapping[str, Any]) -> str:
    forward = to_float(row.get("actual_forward_return"))
    drawdown = to_float(row.get("actual_max_drawdown"))
    vol = to_float(row.get("actual_realized_volatility"))
    if drawdown <= DRAWDOWN_REGIME_THRESHOLD:
        return "drawdown"
    if vol >= HIGH_VOLATILITY_REGIME_THRESHOLD:
        return "high_volatility"
    if forward >= UPTREND_RETURN_THRESHOLD:
        return "uptrend"
    if forward <= DOWNTREND_RETURN_THRESHOLD:
        return "downtrend"
    if vol <= LOW_VOLATILITY_REGIME_THRESHOLD:
        return "low_volatility"
    return "range_bound"


def _regime_label(*, eligible_count: int, score: float, minimum_regime_sample: int) -> str:
    if eligible_count < minimum_regime_sample:
        return "REGIME_INSUFFICIENT_SAMPLE"
    if score > 0.0:
        return "REGIME_SPECIFIC_WEAK_EDGE"
    if score < 0.0:
        return "REGIME_SPECIFIC_FAILURE"
    return "REGIME_MIXED"


def _candidate_reasons(
    *,
    candidate_id: str,
    density_rows: Sequence[Mapping[str, Any]],
    horizon_asset_rows: Sequence[Mapping[str, Any]],
    direction_rows: Sequence[Mapping[str, Any]],
    false_cost_rows: Sequence[Mapping[str, Any]],
    overlap_rows: Sequence[Mapping[str, Any]],
    data_quality_impact_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
    scorecard: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    candidate_density = _candidate_filter(density_rows, candidate_id)
    if any(
        row.get("diagnostic_label") == "POSSIBLE_OVER_NEUTRALIZATION" for row in candidate_density
    ):
        reasons.append("OVER_NEUTRALIZED_SIGNAL")
    if any(
        row.get("diagnostic_label") == "LOW_CONVICTION_SIGNAL_DESIGN" for row in candidate_density
    ):
        reasons.append("LOW_CONFIDENCE_SIGNAL")
    candidate_horizon = _candidate_filter(horizon_asset_rows, candidate_id)
    labels = [label for row in candidate_horizon for label in row.get("diagnostic_labels", [])]
    if "MIXED_BY_HORIZON" in labels:
        reasons.append("HORIZON_MISMATCH")
    if "MIXED_BY_ASSET" in labels:
        reasons.append("ASSET_SPECIFIC_MIXED_RESULTS")
    if any(row.get("diagnostic_label") == "LOCAL_WEAK_EDGE" for row in candidate_horizon):
        reasons.append("REGIME_SPECIFIC_ONLY")
    false_cost = next(iter(_candidate_filter(false_cost_rows, candidate_id)), {})
    if false_cost.get("diagnostic_label") == "FALSE_RISK_OFF_COST_TOO_HIGH":
        reasons.append("FALSE_RISK_OFF_COST_TOO_HIGH")
    if false_cost.get("diagnostic_label") == "FALSE_RISK_ON_COST_TOO_HIGH":
        reasons.append("FALSE_RISK_ON_COST_TOO_HIGH")
    if false_cost.get("diagnostic_label") == "BOTH_FALSE_SIGNAL_COSTS_HIGH":
        reasons.extend(["FALSE_RISK_OFF_COST_TOO_HIGH", "FALSE_RISK_ON_COST_TOO_HIGH"])
    if any(
        row.get("diagnostic_label")
        in {
            "RISK_ON_DRAWDOWN_STRESS_PRONE",
            "RISK_OFF_MISSES_UPSIDE",
            "NEUTRAL_OVER_COVERS_TREND_WINDOWS",
            "VOLATILITY_COMPRESSION_MISCLASSIFIED",
        }
        for row in _candidate_filter(direction_rows, candidate_id)
    ):
        reasons.append("DIRECTION_MAPPING_WEAK")
    if any(
        row.get("diagnostic_label") == "HIGHLY_REDUNDANT"
        and candidate_id in {row.get("candidate_id_left"), row.get("candidate_id_right")}
        for row in overlap_rows
    ):
        reasons.append("CANDIDATE_REDUNDANT_WITH_OTHER_SIGNALS")
    if any(
        row.get("diagnostic_label") == "DATA_QUALITY_BLOCKS_CONCLUSION"
        for row in _candidate_filter(data_quality_impact_rows, candidate_id)
    ):
        reasons.append("DATA_QUALITY_LIMITED")
    if any(
        row.get("diagnostic_label") in {"REGIME_SPECIFIC_WEAK_EDGE", "REGIME_SPECIFIC_FAILURE"}
        for row in _candidate_filter(regime_rows, candidate_id)
    ):
        reasons.append("REGIME_SPECIFIC_ONLY")
    if to_float(scorecard.get("validation_eligible_record_count")) < MINIMUM_LOCAL_SAMPLE:
        reasons.append("INSUFFICIENT_EFFECTIVE_SAMPLE")
    if (
        to_float(scorecard.get("alignment_rate")) < 0.40
        and to_float(scorecard.get("confidence_weighted_alignment_score")) <= 0.0
    ):
        reasons.append("NO_MEASURABLE_EDGE")
    return _unique_ordered(reasons) or ["NO_MEASURABLE_EDGE"]


def _candidate_filter(rows: Sequence[Mapping[str, Any]], candidate_id: str) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("candidate_id")) == candidate_id]


def _unique_ordered(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _recommended_next_action(primary_reason: str) -> str:
    mapping = {
        "OVER_NEUTRALIZED_SIGNAL": "REFINE_SIGNAL_DIRECTION_MAPPING",
        "LOW_CONFIDENCE_SIGNAL": "REFINE_CONFIDENCE_SCALING",
        "HORIZON_MISMATCH": "REFINE_HORIZON_TARGETING",
        "ASSET_SPECIFIC_MIXED_RESULTS": "REFINE_ASSET_SCOPE",
        "DIRECTION_MAPPING_WEAK": "REFINE_SIGNAL_DIRECTION_MAPPING",
        "FALSE_RISK_OFF_COST_TOO_HIGH": "REFINE_RISK_CAP_USAGE",
        "FALSE_RISK_ON_COST_TOO_HIGH": "REFINE_SIGNAL_DIRECTION_MAPPING",
        "CANDIDATE_REDUNDANT_WITH_OTHER_SIGNALS": "MERGE_OR_DROP_REDUNDANT_CANDIDATE",
        "DATA_QUALITY_LIMITED": "CONTINUE_FORWARD_OBSERVE_ONLY",
        "REGIME_SPECIFIC_ONLY": "SPLIT_BY_REGIME",
        "NO_MEASURABLE_EDGE": "REJECT_CURRENT_FORM",
        "INSUFFICIENT_EFFECTIVE_SAMPLE": "CONTINUE_FORWARD_OBSERVE_ONLY",
    }
    return mapping.get(primary_reason, "READY_FOR_REFINED_VALIDATION_DESIGN")


def _refinement_priority(primary_reason: str, action: str) -> str:
    if action in {"REJECT_CURRENT_FORM", "MERGE_OR_DROP_REDUNDANT_CANDIDATE"}:
        return "LOW"
    if primary_reason in {
        "OVER_NEUTRALIZED_SIGNAL",
        "DIRECTION_MAPPING_WEAK",
        "FALSE_RISK_OFF_COST_TOO_HIGH",
        "FALSE_RISK_ON_COST_TOO_HIGH",
    }:
        return "HIGH"
    if primary_reason in {
        "HORIZON_MISMATCH",
        "ASSET_SPECIFIC_MIXED_RESULTS",
        "REGIME_SPECIFIC_ONLY",
    }:
        return "MEDIUM"
    return "LOW"


def _diagnostic_summary_text(primary_reason: str, action: str) -> str:
    return (
        f"Primary inconclusive reason is {primary_reason}; recommended next action is "
        f"{action}. This is research-only and promotion blocked."
    )


def _next_task_recommendation(recommendation_rows: Sequence[Mapping[str, Any]]) -> str:
    actions = {str(row.get("recommended_next_action")) for row in recommendation_rows}
    if actions & {
        "REFINE_SIGNAL_DIRECTION_MAPPING",
        "REFINE_CONFIDENCE_SCALING",
        "REFINE_HORIZON_TARGETING",
        "REFINE_ASSET_SCOPE",
        "REFINE_RISK_CAP_USAGE",
        "SPLIT_BY_REGIME",
        "REGENERATE_WITH_STRONGER_SIGNAL_SPEC",
    }:
        return "TRADING-2287_Candidate_Generator_Refinement_Plan"
    if "READY_FOR_REFINED_VALIDATION_DESIGN" in actions:
        return "TRADING-2287_Refined_Validation_Design"
    return "TRADING-2287_Forward_Observe_Diagnostic_Plan"


def _summary_status(summary_payload: Mapping[str, Any]) -> str:
    nested = summary_payload.get("summary")
    if isinstance(nested, Mapping):
        return str(nested.get("status") or summary_payload.get("status") or "")
    return str(summary_payload.get("status") or "")


def _render_diagnostics_report(
    summary: Mapping[str, Any],
    recommendation_rows: Sequence[Mapping[str, Any]],
    *,
    density_rows: Sequence[Mapping[str, Any]],
    horizon_asset_rows: Sequence[Mapping[str, Any]],
    direction_rows: Sequence[Mapping[str, Any]],
    false_cost_rows: Sequence[Mapping[str, Any]],
    overlap_rows: Sequence[Mapping[str, Any]],
    data_quality_impact_rows: Sequence[Mapping[str, Any]],
) -> str:
    density_counts = Counter(str(row.get("diagnostic_label")) for row in density_rows)
    local_counts = Counter(str(row.get("diagnostic_label")) for row in horizon_asset_rows)
    direction_counts = Counter(str(row.get("diagnostic_label")) for row in direction_rows)
    lines = [
        "# Regenerated Candidate Inconclusive Diagnostics Report",
        "",
        "最后更新：2026-06-30",
        "",
        "## Summary",
        "",
        f"- status: `{summary.get('status')}`",
        f"- candidate_count: `{summary.get('candidate_count')}`",
        f"- input_actual_path_record_count: `{summary.get('input_actual_path_record_count')}`",
        f"- input_eligible_record_count: `{summary.get('input_eligible_record_count')}`",
        "- 2285 candidate states: all three regenerated candidates remained "
        "`ACTUAL_PATH_VALIDATED_INCONCLUSIVE`.",
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "## Candidate Recommendations",
        "",
        "|candidate_id|primary_reason|recommended_next_action|priority|",
        "|---|---|---|---|",
    ]
    for row in recommendation_rows:
        lines.append(
            "|`{}`|`{}`|`{}`|`{}`|".format(
                row.get("candidate_id"),
                row.get("primary_inconclusive_reason"),
                row.get("recommended_next_action"),
                row.get("refinement_priority"),
            )
        )
    lines.extend(
        [
            "",
            "## Diagnostic Findings",
            "",
            f"- signal_density_labels: `{dict(density_counts)}`",
            f"- horizon_asset_labels: `{dict(local_counts)}`",
            f"- direction_alignment_labels: `{dict(direction_counts)}`",
            "- over-neutralization: no group exceeded the neutral dominance threshold unless "
            "`POSSIBLE_OVER_NEUTRALIZATION` appears above.",
            "- local weak edge: inspect `LOCAL_WEAK_EDGE` rows before any generator refinement; "
            "they are not promotion evidence.",
            "",
            "## False Signal Cost",
            "",
            "|candidate_id|label|false_risk_on_cost|false_risk_off_cost|cost_asymmetry|",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in false_cost_rows:
        lines.append(
            "|`{}`|`{}`|{}|{}|{}|".format(
                row.get("candidate_id"),
                row.get("diagnostic_label"),
                row.get("false_risk_on_cost_total"),
                row.get("false_risk_off_cost_total"),
                row.get("cost_asymmetry"),
            )
        )
    lines.extend(
        [
            "",
            "## Candidate Overlap",
            "",
            "|candidate_pair|label|value_corr|direction_agreement|",
            "|---|---|---:|---:|",
        ]
    )
    for row in overlap_rows:
        lines.append(
            "|`{} / {}`|`{}`|{}|{}|".format(
                row.get("candidate_id_left"),
                row.get("candidate_id_right"),
                row.get("diagnostic_label"),
                row.get("signal_value_correlation"),
                row.get("signal_direction_agreement_rate"),
            )
        )
    lines.extend(
        [
            "",
            "## Data Quality Impact",
            "",
            "|candidate_id|label|warning_rate|eligible_ratio|score_delta_due_to_warnings|",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in data_quality_impact_rows:
        lines.append(
            "|`{}`|`{}`|{}|{}|{}|".format(
                row.get("candidate_id"),
                row.get("diagnostic_label"),
                row.get("warning_rate"),
                row.get("eligible_ratio"),
                row.get("score_delta_due_to_warnings"),
            )
        )
    lines.extend(
        [
            "",
            "## Next Route",
            "",
            f"- next_task_recommendation: `{summary.get('next_task_recommendation')}`",
            "",
            "",
            "本报告只诊断 inconclusive 原因和 signal utility；不生成新 signal、不修改 "
            "TRADING-2284 artifacts、不重跑 TRADING-2285 actual-path validation，也不代表 "
            "owner approval、promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_utility_drilldown_doc(
    summary: Mapping[str, Any],
    density_rows: Sequence[Mapping[str, Any]],
    horizon_asset_rows: Sequence[Mapping[str, Any]],
) -> str:
    density_counts = Counter(str(row.get("diagnostic_label")) for row in density_rows)
    local_counts = Counter(str(row.get("diagnostic_label")) for row in horizon_asset_rows)
    return "\n".join(
        [
            "# Regenerated Candidate Signal Utility Drilldown",
            "",
            "最后更新：2026-06-30",
            "",
            f"- diagnostic_matrix_count: `{summary.get('diagnostic_matrix_count')}`",
            f"- signal_density_labels: `{dict(density_counts)}`",
            f"- horizon_asset_labels: `{dict(local_counts)}`",
            "- diagnostic-only regime labels are not promotion evidence.",
            "- promotion_allowed: `false`",
            "- paper_shadow_allowed: `false`",
            "- production_allowed: `false`",
            "- broker_action: `none`",
            "",
        ]
    )


def _render_refinement_recommendation_doc(
    summary: Mapping[str, Any],
    recommendation_rows: Sequence[Mapping[str, Any]],
    *,
    false_cost_rows: Sequence[Mapping[str, Any]],
    overlap_rows: Sequence[Mapping[str, Any]],
    data_quality_impact_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Regenerated Candidate Refinement Recommendation",
        "",
        "最后更新：2026-06-30",
        "",
        f"- next_task_recommendation: `{summary.get('next_task_recommendation')}`",
        f"- candidate_recommendation_count: `{len(recommendation_rows)}`",
        "- recommendations remain research-only and cannot emit promotion, paper-shadow, "
        "production, or broker-ready states.",
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "## Recommendations",
        "",
        "|candidate_id|primary_reason|secondary_reasons|next_action|priority|",
        "|---|---|---|---|---|",
    ]
    for row in recommendation_rows:
        lines.append(
            "|`{}`|`{}`|`{}`|`{}`|`{}`|".format(
                row.get("candidate_id"),
                row.get("primary_inconclusive_reason"),
                row.get("secondary_inconclusive_reasons"),
                row.get("recommended_next_action"),
                row.get("refinement_priority"),
            )
        )
    lines.extend(
        [
            "",
            "## Supporting Diagnostics",
            "",
            f"- false_signal_cost_labels: `{_label_counts(false_cost_rows)}`",
            f"- overlap_labels: `{_label_counts(overlap_rows)}`",
            f"- data_quality_impact_labels: `{_label_counts(data_quality_impact_rows)}`",
            "",
        ]
    )
    return "\n".join(lines)


def _label_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(row.get("diagnostic_label")) for row in rows))


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    numeric = to_float(value)
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        items = [str(item).strip() for item in value]
    return tuple(item for item in items if item)


def _safety_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
