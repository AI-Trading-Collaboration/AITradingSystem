from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_signal_generator import (
    candidate_artifact_safety_fields,
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    clean_for_yaml,
    round_float,
    to_float,
    validate_cached_market_data,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    DOWNSIDE_TAIL_DRAWDOWN_THRESHOLD,
    DRAWDOWN_STRESS_THRESHOLD,
    MAX_ALLOWED_DRAWDOWN_THRESHOLD,
    MILD_DRAWDOWN_THRESHOLD,
    NEGATIVE_RETURN_THRESHOLD,
    POSITIVE_RETURN_THRESHOLD,
    STRESS_REALIZED_VOLATILITY_THRESHOLD,
    STRONG_POSITIVE_RETURN_THRESHOLD,
    UPSIDE_BREAKOUT_RETURN_THRESHOLD,
    VOLATILITY_EXPANSION_ALIGNMENT_THRESHOLD,
    calculate_actual_path,
    load_actual_price_matrix,
)
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    ARTIFACT_ROLE as SCOPE_NARROWED_INPUT_ARTIFACT_ROLE,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    RISK_APPETITE_ARCHIVE_CANDIDATE,
)

DEFAULT_SCOPE_NARROWED_GENERATOR_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "scope_narrowed_candidate_generators_regenerated"
)
DEFAULT_SCOPE_REVIEW_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "refined_candidate_local_edge_scope_review"
)
DEFAULT_REFINED_VALIDATION_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "refined_candidate_actual_path_validation"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "scope_narrowed_candidate_actual_path_validation"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2292_SCOPE_NARROWED_CANDIDATE_ACTUAL_PATH_VALIDATION"
STATUS = "SCOPE_NARROWED_ACTUAL_PATH_VALIDATION_READY_PROMOTION_BLOCKED"
MODE = "scope_narrowed_actual_path_validation"
ARTIFACT_ROLE = "scope_narrowed_candidate_actual_path_validation"
INPUT_SCOPE_NARROWING_TASK = "TRADING-2291"

CONFIRMATION_CANDIDATE_ID = "baseline_plus_trend_structure_scope_narrowed_confirmation_v1"
RISK_CAP_CANDIDATE_ID = "volatility_regime_scope_narrowed_risk_cap_v1"
DEFAULT_INCLUDE_CANDIDATES = (CONFIRMATION_CANDIDATE_ID, RISK_CAP_CANDIDATE_ID)
DEFAULT_ARCHIVED_CANDIDATES = (RISK_APPETITE_ARCHIVE_CANDIDATE,)
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")

USAGE_BY_CANDIDATE = {
    CONFIRMATION_CANDIDATE_ID: "confirmation_only",
    RISK_CAP_CANDIDATE_ID: "risk_cap_only",
}
RISK_CAP_VALIDATION_DIRECTIONS = {
    "risk_off",
    "trend_weakening",
    "volatility_expansion",
}
CONFIRMATION_VALIDATION_DIRECTIONS = {
    "risk_on",
    "risk_off",
    "trend_confirming",
    "trend_weakening",
}
BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}
ALLOWED_CONFIRMATION_STATUSES = {
    "CONFIRMATION_SCOPE_VALIDATED_CONTINUE_RESEARCH",
    "CONFIRMATION_SCOPE_VALIDATED_LOCAL_EDGE",
    "CONFIRMATION_SCOPE_VALIDATED_INCONCLUSIVE",
    "CONFIRMATION_SCOPE_VALIDATED_REJECT_RECOMMENDED",
    "CONFIRMATION_SCOPE_DATA_QUALITY_BLOCKED",
    "CONFIRMATION_SCOPE_SAMPLE_BLOCKED",
}
ALLOWED_RISK_CAP_STATUSES = {
    "RISK_CAP_SCOPE_VALIDATED_CONTINUE_RESEARCH",
    "RISK_CAP_SCOPE_VALIDATED_LOCAL_EDGE",
    "RISK_CAP_SCOPE_VALIDATED_INCONCLUSIVE",
    "RISK_CAP_SCOPE_VALIDATED_REJECT_RECOMMENDED",
    "RISK_CAP_SCOPE_DATA_QUALITY_BLOCKED",
    "RISK_CAP_SCOPE_SAMPLE_BLOCKED",
}
ALLOWED_STATE_RECOMMENDATIONS = {
    "SCOPE_NARROWED_VALIDATED_LOCAL_EDGE",
    "SCOPE_NARROWED_VALIDATED_CONTINUE_RESEARCH",
    "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE",
    "SCOPE_NARROWED_VALIDATED_INCONCLUSIVE",
    "SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED",
    "SCOPE_NARROWED_SAMPLE_BLOCKED",
    "SCOPE_NARROWED_DATA_QUALITY_BLOCKED",
}

REQUIRED_SCOPE_NARROWED_TOP_LEVEL_FILES = {
    "run_summary": "scope_narrowed_regeneration_run_summary.json",
    "validation_summary": "scope_narrowed_regeneration_validation_summary.json",
    "registry": "scope_narrowed_candidate_registry.json",
    "delta_summary": "scope_narrowed_original_vs_refined_vs_scope_delta_summary.json",
}
REQUIRED_SCOPE_NARROWED_CANDIDATE_FILES = {
    "signal_spec": "scope_narrowed_candidate_signal_spec.json",
    "signal_series": "scope_narrowed_candidate_signal_series.csv",
    "prediction_artifact": "scope_narrowed_candidate_prediction_artifact.json",
    "generation_summary": "scope_narrowed_generation_summary.json",
    "validation_summary": "scope_narrowed_validation_summary.json",
    "scope_filter_report": "scope_filter_report.json",
    "lineage_report": "scope_narrowed_lineage_report.json",
    "delta": "refined_vs_scope_narrowed_delta.json",
}
REQUIRED_SCOPE_REVIEW_FILES = {
    "summary": "local_edge_scope_review_summary.json",
    "scope_recommendation": "candidate_scope_narrowing_recommendation_matrix.json",
    "direction_scope": "candidate_direction_scope_matrix.json",
    "high_conviction_scope": "candidate_high_conviction_scope_matrix.json",
    "false_cost_scope": "candidate_false_cost_scope_matrix.json",
    "next_task": "candidate_next_task_recommendation_matrix.json",
    "decision_summary": "candidate_scope_review_decision_summary.json",
}
REQUIRED_REFINED_VALIDATION_FILES = {
    "summary": "refined_candidate_actual_path_validation_summary.json",
    "scorecard": "refined_candidate_validation_scorecard.json",
    "high_conviction": "refined_high_conviction_outcome_drilldown.json",
    "false_cost": "refined_false_signal_cost_matrix.json",
    "comparison": "original_vs_refined_actual_path_comparison.json",
    "state": "refined_candidate_state_recommendation_matrix.json",
    "data_quality": "refined_candidate_data_quality_report.json",
}

# Research-only pilot baselines for TRADING-2292 evidence classification. These
# are not promotion, paper-shadow, production, or broker gates.
MINIMUM_DATA_COVERAGE_RATIO = 0.8
CONFIRMATION_MINIMUM_ACTIVE_ELIGIBLE_RECORDS = 500
CONFIRMATION_MINIMUM_ASSET_HORIZON_BUCKET_RECORDS = 30
RISK_CAP_MINIMUM_ACTIVE_ELIGIBLE_RECORDS = 100
RISK_CAP_MINIMUM_ASSET_HORIZON_BUCKET_RECORDS = 10
ACTIVE_VS_INACTIVE_STRONG_DELTA = 0.05
ACTIVE_VS_INACTIVE_WEAK_DELTA = 0.0
FALSE_COST_ACCEPTABLE_PER_RECORD = 0.08
MISSED_UPSIDE_COST_ACCEPTABLE_PER_RECORD = 0.08
FORWARD_OBSERVE_MINIMUM_ALIGNMENT_DELTA = 0.02
LOCAL_EDGE_MINIMUM_ALIGNMENT_DELTA = 0.0

VALIDATOR_TRACE_SAMPLE_LIMIT = 512


class ScopeNarrowedCandidateActualPathValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ScopeNarrowedCandidateArtifacts:
    scope_narrowed_candidate_id: str
    refined_candidate_id: str
    original_candidate_id: str
    usage_role: str
    candidate_dir: Path
    signal_spec: dict[str, Any]
    signal_series: list[dict[str, Any]]
    prediction_artifact: dict[str, Any]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    scope_filter_report: dict[str, Any]
    lineage_report: dict[str, Any]
    delta: dict[str, Any]
    artifact_paths: dict[str, Path]
    input_artifact_validation_status: str

    @property
    def prediction_records(self) -> list[dict[str, Any]]:
        rows = self.prediction_artifact.get("prediction_records")
        if not isinstance(rows, list):
            return []
        return [dict(row) for row in rows if isinstance(row, Mapping)]


@dataclass(frozen=True)
class ScopeNarrowedActualPathInputs:
    scope_narrowed_generator_dir: Path
    scope_review_dir: Path
    refined_validation_dir: Path
    include_candidates: tuple[str, ...]
    archived_candidates: tuple[str, ...]
    top_level_payloads: dict[str, dict[str, Any]]
    scope_review_payloads: dict[str, dict[str, Any]]
    refined_validation_payloads: dict[str, dict[str, Any]]
    scope_narrowed_artifacts: dict[str, ScopeNarrowedCandidateArtifacts]
    risk_appetite_archive_record: dict[str, Any]


def run_scope_narrowed_candidate_actual_path_validation(
    *,
    scope_narrowed_generator_dir: Path = DEFAULT_SCOPE_NARROWED_GENERATOR_ROOT,
    scope_review_dir: Path = DEFAULT_SCOPE_REVIEW_ROOT,
    refined_validation_dir: Path = DEFAULT_REFINED_VALIDATION_ROOT,
    include_candidates: Sequence[str] | str = DEFAULT_INCLUDE_CANDIDATES,
    archived_candidates: Sequence[str] | str = DEFAULT_ARCHIVED_CANDIDATES,
    target_assets: Sequence[str] | str = DEFAULT_TARGET_ASSETS,
    horizons: Sequence[str] | str = DEFAULT_HORIZONS,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise ScopeNarrowedCandidateActualPathValidationError(
            "scope-narrowed candidate actual-path validation only supports "
            f"{MODE} mode"
        )
    include_ids = _normalize_list(include_candidates)
    archive_ids = _normalize_list(archived_candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=None,
        expected_price_tickers=asset_ids,
        expected_rate_series=(),
    )
    price_matrix = load_actual_price_matrix(prices_path, asset_ids)
    inputs = load_scope_narrowed_actual_path_inputs(
        scope_narrowed_generator_dir=scope_narrowed_generator_dir,
        scope_review_dir=scope_review_dir,
        refined_validation_dir=refined_validation_dir,
        include_candidates=include_ids,
        archived_candidates=archive_ids,
    )

    active_actual_rows: list[dict[str, Any]] = []
    active_outcome_rows: list[dict[str, Any]] = []
    inactive_outcome_rows: list[dict[str, Any]] = []
    inactive_actual_rows: list[dict[str, Any]] = []
    excluded_active_rows: list[dict[str, Any]] = []

    for candidate_id in include_ids:
        bundle = inputs.scope_narrowed_artifacts[candidate_id]
        for record in bundle.prediction_records:
            if str(record.get("target_asset")) not in asset_ids:
                continue
            if str(record.get("horizon")) not in horizon_ids:
                continue
            prepared = _scope_narrowed_prediction_record(record, bundle)
            actual_path = calculate_actual_path(
                prepared,
                price_matrix,
                minimum_data_coverage_ratio=MINIMUM_DATA_COVERAGE_RATIO,
            )
            active_validation = _is_active_validation_record(prepared)
            alignment = classify_scope_narrowed_alignment(prepared, actual_path)
            actual_row = scope_narrowed_actual_path_row(prepared, actual_path)
            outcome_row = scope_narrowed_prediction_outcome_row(
                prepared,
                actual_path,
                alignment,
            )
            if active_validation:
                active_actual_rows.append(actual_row)
                active_outcome_rows.append(outcome_row)
            elif _bool(prepared.get("scope_active")):
                excluded_active_rows.append(outcome_row)
                inactive_actual_rows.append(actual_row)
                inactive_outcome_rows.append(outcome_row)
            else:
                inactive_actual_rows.append(actual_row)
                inactive_outcome_rows.append(outcome_row)

    data_quality_rows = [
        build_scope_narrowed_data_quality_row(
            scope_narrowed_candidate_id=candidate_id,
            usage_role=inputs.scope_narrowed_artifacts[candidate_id].usage_role,
            actual_path_rows=_candidate_rows(
                [*active_actual_rows, *inactive_actual_rows],
                candidate_id,
            ),
            input_artifact_validation_status=inputs.scope_narrowed_artifacts[
                candidate_id
            ].input_artifact_validation_status,
        )
        for candidate_id in include_ids
    ]
    inactive_reference_rows = build_scope_narrowed_inactive_reference_matrix(
        inactive_outcome_rows
    )
    confirmation_scorecards = build_confirmation_only_validation_scorecard(
        active_outcome_rows,
        inactive_outcome_rows,
    )
    risk_cap_scorecards = build_risk_cap_only_validation_scorecard(
        active_outcome_rows,
        inactive_outcome_rows,
    )
    scorecards = [*confirmation_scorecards, *risk_cap_scorecards]
    sample_rows = build_scope_narrowed_sample_sufficiency_report(
        active_outcome_rows,
        candidate_ids=include_ids,
        usage_by_candidate={
            candidate_id: inputs.scope_narrowed_artifacts[candidate_id].usage_role
            for candidate_id in include_ids
        },
    )
    comparison_rows = build_scope_narrowed_active_vs_inactive_comparison(
        active_outcome_rows=active_outcome_rows,
        inactive_outcome_rows=inactive_outcome_rows,
        scorecards=scorecards,
        sample_rows=sample_rows,
        data_quality_rows=data_quality_rows,
    )
    false_cost_rows = build_scope_narrowed_false_signal_cost_matrix(active_outcome_rows)
    state_rows = build_scope_narrowed_state_recommendation_matrix(
        comparison_rows=comparison_rows,
        scorecards=scorecards,
        sample_rows=sample_rows,
        data_quality_rows=data_quality_rows,
    )
    archive_carry_forward = build_risk_appetite_archive_carry_forward(
        inputs.risk_appetite_archive_record
    )
    error_rows = [
        scope_narrowed_error_attribution_row(row)
        for row in [*active_outcome_rows, *excluded_active_rows]
    ]
    summary = _summary(
        include_candidates=include_ids,
        archived_candidates=archive_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        generated_at=generated_at,
        active_actual_rows=active_actual_rows,
        active_outcome_rows=active_outcome_rows,
        inactive_outcome_rows=inactive_outcome_rows,
        scorecards=scorecards,
        data_quality_rows=data_quality_rows,
        comparison_rows=comparison_rows,
        state_rows=state_rows,
        source_data_quality=data_quality,
        scope_narrowed_generator_dir=scope_narrowed_generator_dir,
        scope_review_dir=scope_review_dir,
        refined_validation_dir=refined_validation_dir,
        prices_path=prices_path,
    )
    common = _common_payload(generated_at=generated_at, summary=summary, mode=mode)
    paths = _artifact_paths(output_dir=output_dir, docs_root=docs_root)
    payloads = _build_output_payloads(
        common=common,
        summary=summary,
        active_actual_rows=active_actual_rows,
        active_outcome_rows=active_outcome_rows,
        inactive_reference_rows=inactive_reference_rows,
        comparison_rows=comparison_rows,
        confirmation_scorecards=confirmation_scorecards,
        risk_cap_scorecards=risk_cap_scorecards,
        sample_rows=sample_rows,
        false_cost_rows=false_cost_rows,
        state_rows=state_rows,
        error_rows=error_rows,
        data_quality_rows=data_quality_rows,
        archive_carry_forward=archive_carry_forward,
    )
    for key, payload in payloads.items():
        _assert_generated_payload_safe(key, payload)

    write_json(paths["summary"], payloads["summary"])
    write_json(paths["active_actual_path_json"], payloads["active_actual_path"])
    write_csv_rows(paths["active_actual_path_csv"], active_actual_rows)
    write_json(paths["active_outcome_json"], payloads["active_outcome"])
    write_csv_rows(paths["active_outcome_csv"], active_outcome_rows)
    write_json(paths["inactive_reference_json"], payloads["inactive_reference"])
    write_csv_rows(paths["inactive_reference_csv"], inactive_reference_rows)
    write_json(paths["comparison_json"], payloads["comparison"])
    write_csv_rows(paths["comparison_csv"], comparison_rows)
    write_json(paths["confirmation_scorecard_json"], payloads["confirmation_scorecard"])
    write_csv_rows(paths["confirmation_scorecard_csv"], confirmation_scorecards)
    write_json(paths["risk_cap_scorecard_json"], payloads["risk_cap_scorecard"])
    write_csv_rows(paths["risk_cap_scorecard_csv"], risk_cap_scorecards)
    write_json(paths["sample_sufficiency_json"], payloads["sample_sufficiency"])
    write_csv_rows(paths["sample_sufficiency_csv"], sample_rows)
    write_json(paths["false_signal_cost_json"], payloads["false_signal_cost"])
    write_csv_rows(paths["false_signal_cost_csv"], false_cost_rows)
    write_json(paths["state_recommendation_json"], payloads["state_recommendation"])
    write_csv_rows(paths["state_recommendation_csv"], state_rows)
    write_json(paths["error_attribution_seed"], payloads["error_attribution_seed"])
    write_json(paths["data_quality_report"], payloads["data_quality_report"])
    write_json(paths["risk_appetite_archive_carry_forward"], archive_carry_forward)
    write_markdown(
        paths["validation_report_doc"],
        _render_validation_report(summary, comparison_rows, state_rows),
    )
    write_markdown(
        paths["confirmation_doc"],
        _render_confirmation_doc(confirmation_scorecards),
    )
    write_markdown(paths["risk_cap_doc"], _render_risk_cap_doc(risk_cap_scorecards))
    write_markdown(paths["comparison_doc"], _render_comparison_doc(comparison_rows))
    write_markdown(paths["state_doc"], _render_state_doc(state_rows, summary))

    result = clean_for_yaml(
        {
            **common,
            "summary": summary,
            "candidate_scorecards": scorecards,
            "candidate_state_recommendations": state_rows,
            "candidate_data_quality": data_quality_rows,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "scope_narrowed_candidate_actual_path_validation_cli": "implemented",
            "baseline_confirmation_candidate_validated": True,
            "volatility_risk_cap_candidate_validated": True,
            "risk_appetite_archive_carried_forward": True,
            "active_actual_path_matrix_generated": True,
            "active_prediction_outcome_matrix_generated": True,
            "inactive_reference_matrix_generated": True,
            "active_vs_inactive_comparison_generated": True,
            "confirmation_only_scorecard_generated": True,
            "risk_cap_only_scorecard_generated": True,
            "sample_sufficiency_report_generated": True,
            "state_recommendation_matrix_generated": True,
            "risk_appetite_included_in_validation": False,
            "trading_2281_permanently_inconclusive_decisions_changed": False,
            "trading_2285_original_inconclusive_decisions_changed": False,
            "trading_2289_refined_state_decisions_changed": False,
            "trading_2291_scope_narrowing_decisions_changed": False,
        }
    )
    _assert_generated_payload_safe("scope_narrowed_actual_path_validation_result", result)
    return result


def load_scope_narrowed_actual_path_inputs(
    *,
    scope_narrowed_generator_dir: Path,
    scope_review_dir: Path,
    refined_validation_dir: Path,
    include_candidates: Sequence[str] | str,
    archived_candidates: Sequence[str] | str,
) -> ScopeNarrowedActualPathInputs:
    include_ids = _normalize_list(include_candidates)
    archive_ids = _normalize_list(archived_candidates)
    _validate_requested_candidates(include_ids, archive_ids)
    top_level_payloads = {
        key: _load_json_required(
            scope_narrowed_generator_dir / filename,
            f"scope_narrowed_generator.{key}",
        )
        for key, filename in REQUIRED_SCOPE_NARROWED_TOP_LEVEL_FILES.items()
    }
    scope_review_payloads = {
        key: _load_json_required(scope_review_dir / filename, f"scope_review.{key}")
        for key, filename in REQUIRED_SCOPE_REVIEW_FILES.items()
    }
    refined_validation_payloads = {
        key: _load_json_required(
            refined_validation_dir / filename,
            f"refined_validation.{key}",
        )
        for key, filename in REQUIRED_REFINED_VALIDATION_FILES.items()
    }
    artifacts = load_scope_narrowed_candidate_artifacts(
        scope_narrowed_generator_dir,
        include_ids,
    )
    archive_record = _load_json_required(
        scope_narrowed_generator_dir
        / "risk_appetite_archive"
        / "risk_appetite_current_form_archive_record.json",
        "risk_appetite_archive.current_form_record",
    )
    _validate_top_level_state(top_level_payloads, include_ids)
    _validate_scope_review_context(scope_review_payloads, include_ids)
    _validate_refined_validation_context(refined_validation_payloads, include_ids)
    _validate_risk_appetite_archive_record(archive_record, archive_ids)
    return ScopeNarrowedActualPathInputs(
        scope_narrowed_generator_dir=scope_narrowed_generator_dir,
        scope_review_dir=scope_review_dir,
        refined_validation_dir=refined_validation_dir,
        include_candidates=include_ids,
        archived_candidates=archive_ids,
        top_level_payloads=top_level_payloads,
        scope_review_payloads=scope_review_payloads,
        refined_validation_payloads=refined_validation_payloads,
        scope_narrowed_artifacts=artifacts,
        risk_appetite_archive_record=archive_record,
    )


def load_scope_narrowed_candidate_artifacts(
    scope_narrowed_generator_dir: Path,
    candidates: Sequence[str] | str,
) -> dict[str, ScopeNarrowedCandidateArtifacts]:
    candidate_ids = _normalize_list(candidates)
    loaded: dict[str, ScopeNarrowedCandidateArtifacts] = {}
    for candidate_id in candidate_ids:
        candidate_dir = scope_narrowed_generator_dir / candidate_id
        paths = {
            key: candidate_dir / filename
            for key, filename in REQUIRED_SCOPE_NARROWED_CANDIDATE_FILES.items()
        }
        missing = [str(path) for path in paths.values() if not path.exists()]
        if missing:
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{candidate_id}: missing scope-narrowed artifact(s): {missing}"
            )
        signal_spec = _read_json_object(paths["signal_spec"])
        signal_series = _read_signal_series_csv(paths["signal_series"])
        prediction_artifact = _read_json_object(paths["prediction_artifact"])
        generation_summary = _read_json_object(paths["generation_summary"])
        validation_summary = _read_json_object(paths["validation_summary"])
        scope_filter_report = _read_json_object(paths["scope_filter_report"])
        lineage_report = _read_json_object(paths["lineage_report"])
        delta = _read_json_object(paths["delta"])
        usage_role = str(
            prediction_artifact.get("usage_role")
            or scope_filter_report.get("usage_role")
            or USAGE_BY_CANDIDATE.get(candidate_id, "")
        )
        refined_candidate_id = str(
            prediction_artifact.get("refined_candidate_id")
            or scope_filter_report.get("refined_candidate_id")
            or ""
        )
        original_candidate_id = str(
            prediction_artifact.get("original_candidate_id")
            or scope_filter_report.get("original_candidate_id")
            or ""
        )
        input_status = validate_scope_narrowed_candidate_artifact_bundle(
            scope_narrowed_candidate_id=candidate_id,
            usage_role=usage_role,
            signal_spec=signal_spec,
            signal_series=signal_series,
            prediction_artifact=prediction_artifact,
            validation_summary=validation_summary,
            scope_filter_report=scope_filter_report,
            lineage_report=lineage_report,
        )
        loaded[candidate_id] = ScopeNarrowedCandidateArtifacts(
            scope_narrowed_candidate_id=candidate_id,
            refined_candidate_id=refined_candidate_id,
            original_candidate_id=original_candidate_id,
            usage_role=usage_role,
            candidate_dir=candidate_dir,
            signal_spec=signal_spec,
            signal_series=signal_series,
            prediction_artifact=prediction_artifact,
            generation_summary=generation_summary,
            validation_summary=validation_summary,
            scope_filter_report=scope_filter_report,
            lineage_report=lineage_report,
            delta=delta,
            artifact_paths=paths,
            input_artifact_validation_status=input_status,
        )
    return loaded


def validate_scope_narrowed_candidate_artifact_bundle(
    *,
    scope_narrowed_candidate_id: str,
    usage_role: str,
    signal_spec: Mapping[str, Any],
    signal_series: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    scope_filter_report: Mapping[str, Any],
    lineage_report: Mapping[str, Any],
) -> str:
    validator = CandidateSignalBindingValidator()
    sample_records = _validator_record_sample(signal_series)
    sampled_artifact = dict(prediction_artifact)
    sampled_artifact["prediction_records"] = _validator_record_sample(
        _records(prediction_artifact.get("prediction_records"))
    )
    errors = (
        list(validator.validate_candidate_signal_spec(signal_spec).errors)
        + list(validator.validate_candidate_bound_signal_series(sample_records).errors)
        + list(
            validator.validate_candidate_bound_prediction_artifact(
                sampled_artifact,
            ).errors
        )
    )
    if signal_spec.get("candidate_id") != scope_narrowed_candidate_id:
        errors.append(f"{scope_narrowed_candidate_id}: signal spec candidate_id mismatch")
    if prediction_artifact.get("candidate_id") != scope_narrowed_candidate_id:
        errors.append(
            f"{scope_narrowed_candidate_id}: prediction artifact candidate_id mismatch"
        )
    if prediction_artifact.get("artifact_role") != SCOPE_NARROWED_INPUT_ARTIFACT_ROLE:
        errors.append(f"{scope_narrowed_candidate_id}: invalid artifact_role")
    if usage_role not in {"confirmation_only", "risk_cap_only"}:
        errors.append(f"{scope_narrowed_candidate_id}: unsupported usage_role {usage_role}")
    if validation_summary.get("status") != "PASS":
        errors.append(f"{scope_narrowed_candidate_id}: validation_summary status is not PASS")
    if not scope_filter_report:
        errors.append(f"{scope_narrowed_candidate_id}: missing scope_filter_report")
    if not lineage_report:
        errors.append(f"{scope_narrowed_candidate_id}: missing lineage_report")
    errors.extend(
        _scope_required_field_errors(
            f"{scope_narrowed_candidate_id}.prediction_artifact",
            prediction_artifact,
            usage_role=usage_role,
            require_scope_fields=False,
        )
    )
    records = _records(prediction_artifact.get("prediction_records"))
    if not records:
        errors.append(f"{scope_narrowed_candidate_id}: prediction_records are empty")
    for index, row in enumerate(records):
        errors.extend(
            _scope_required_field_errors(
                f"{scope_narrowed_candidate_id}.prediction_records[{index}]",
                row,
                usage_role=usage_role,
                require_scope_fields=True,
            )
        )
        if row.get("scope_narrowed_candidate_id") != scope_narrowed_candidate_id:
            errors.append(
                f"{scope_narrowed_candidate_id}.prediction_records[{index}]: "
                "scope_narrowed_candidate_id mismatch"
            )
    if errors:
        raise ScopeNarrowedCandidateActualPathValidationError("; ".join(errors))
    return "PASS"


def scope_narrowed_actual_path_row(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "scope_narrowed_candidate_id": record.get("scope_narrowed_candidate_id"),
            "refined_candidate_id": record.get("refined_candidate_id"),
            "original_candidate_id": record.get("original_candidate_id"),
            "candidate_id": record.get("scope_narrowed_candidate_id")
            or record.get("candidate_id"),
            "usage_role": record.get("usage_role"),
            "target_asset": record.get("target_asset"),
            "horizon": record.get("horizon"),
            "decision_timestamp": record.get("decision_timestamp"),
            "valid_from": record.get("valid_from"),
            "valid_until": record.get("valid_until"),
            "signal_direction": record.get("signal_direction"),
            "scope_active": _bool(record.get("scope_active")),
            "confirmation_score": _optional_float(record.get("confirmation_score")),
            "confirmation_confidence": _optional_float(
                record.get("confirmation_confidence")
            ),
            "risk_cap_score": _optional_float(record.get("risk_cap_score")),
            "risk_cap_intensity": record.get("risk_cap_intensity"),
            **dict(actual_path),
            **_safety_fields(),
        }
    )


def scope_narrowed_prediction_outcome_row(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    alignment: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "scope_narrowed_candidate_id": record.get("scope_narrowed_candidate_id"),
            "refined_candidate_id": record.get("refined_candidate_id"),
            "original_candidate_id": record.get("original_candidate_id"),
            "candidate_id": record.get("scope_narrowed_candidate_id")
            or record.get("candidate_id"),
            "usage_role": record.get("usage_role"),
            "target_asset": record.get("target_asset"),
            "decision_timestamp": record.get("decision_timestamp"),
            "horizon": record.get("horizon"),
            "signal_direction": record.get("signal_direction"),
            "confirmation_direction": _confirmation_direction(record),
            "high_conviction_flag": _bool(record.get("high_conviction_flag")),
            "scope_active": _bool(record.get("scope_active")),
            "active_validation_record": _is_active_validation_record(record),
            "confirmation_score": _optional_float(record.get("confirmation_score")),
            "confirmation_confidence": _optional_float(
                record.get("confirmation_confidence")
            ),
            "risk_cap_score": _optional_float(record.get("risk_cap_score")),
            "risk_cap_intensity": record.get("risk_cap_intensity"),
            "actual_path_status": actual_path.get("actual_path_status"),
            "validation_eligible": actual_path.get("validation_eligible"),
            "data_quality_warning": actual_path.get("data_quality_warning"),
            "actual_forward_return": actual_path.get("forward_return"),
            "actual_max_drawdown": actual_path.get("max_drawdown_during_horizon"),
            "actual_max_runup": actual_path.get("max_runup_during_horizon"),
            "actual_realized_volatility": actual_path.get("realized_volatility"),
            "downside_tail_event": actual_path.get("downside_tail_event"),
            "upside_breakout_event": actual_path.get("upside_breakout_event"),
            "stress_event": actual_path.get("stress_event"),
            "alignment_label": alignment.get("alignment_label"),
            "alignment_score": alignment.get("alignment_score"),
            "error_type": alignment.get("error_type"),
            "dominant_observed_driver": _dominant_observed_driver(actual_path),
            **_safety_fields(),
        }
    )


def classify_scope_narrowed_alignment(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    usage_role = str(record.get("usage_role") or "")
    if usage_role == "confirmation_only":
        return classify_confirmation_alignment(record, actual_path)
    if usage_role == "risk_cap_only":
        return classify_risk_cap_alignment(record, actual_path)
    return _alignment("data_quality_inconclusive", 0.0, "data_quality_inconclusive")


def classify_confirmation_alignment(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(actual_path.get("validation_eligible")):
        return _alignment("data_quality_inconclusive", 0.0, "data_quality_inconclusive")
    direction = _confirmation_direction(record)
    forward_return = _optional_float(actual_path.get("forward_return"))
    max_drawdown = _optional_float(actual_path.get("max_drawdown_during_horizon"))
    stress_event = bool(actual_path.get("stress_event"))
    if direction == "positive_confirmation":
        if (
            stress_event
            or _lte(max_drawdown, DRAWDOWN_STRESS_THRESHOLD)
            or _lte(forward_return, NEGATIVE_RETURN_THRESHOLD)
        ):
            return _alignment("false_confirmation", -1.0, "false_confirmation")
        if _gt(forward_return, POSITIVE_RETURN_THRESHOLD) and _gt(
            max_drawdown,
            MAX_ALLOWED_DRAWDOWN_THRESHOLD,
        ):
            return _alignment("confirmation_aligned", 1.0, "no_error")
        if _gt(forward_return, 0.0) and not stress_event:
            return _alignment("confirmation_partially_aligned", 0.5, "no_error")
        return _alignment("confirmation_inconclusive", 0.0, "no_error")
    if direction == "negative_confirmation":
        if _gte(forward_return, STRONG_POSITIVE_RETURN_THRESHOLD) and _gt(
            max_drawdown,
            MILD_DRAWDOWN_THRESHOLD,
        ):
            return _alignment("false_warning", -1.0, "false_warning")
        if (
            stress_event
            or _lte(max_drawdown, DRAWDOWN_STRESS_THRESHOLD)
            or _lte(forward_return, NEGATIVE_RETURN_THRESHOLD)
        ):
            return _alignment("warning_aligned", 1.0, "no_error")
        if _lte(forward_return, 0.0):
            return _alignment("warning_partially_aligned", 0.5, "no_error")
        return _alignment("warning_inconclusive", 0.0, "no_error")
    return _alignment("confirmation_inactive_reference", 0.0, "no_error")


def classify_risk_cap_alignment(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(actual_path.get("validation_eligible")):
        return _alignment("data_quality_inconclusive", 0.0, "data_quality_inconclusive")
    direction = str(record.get("signal_direction") or "")
    if direction not in RISK_CAP_VALIDATION_DIRECTIONS:
        return _alignment("risk_cap_inactive_reference", 0.0, "no_error")
    forward_return = _optional_float(actual_path.get("forward_return"))
    max_drawdown = _optional_float(actual_path.get("max_drawdown_during_horizon"))
    realized_vol = _optional_float(actual_path.get("realized_volatility")) or 0.0
    stress_event = bool(actual_path.get("stress_event"))
    downside_tail = bool(actual_path.get("downside_tail_event"))
    if _gte(forward_return, STRONG_POSITIVE_RETURN_THRESHOLD) and _gt(
        max_drawdown,
        MILD_DRAWDOWN_THRESHOLD,
    ) and not stress_event:
        return _alignment("false_risk_cap", -1.0, "false_risk_cap")
    if (
        stress_event
        or downside_tail
        or _lte(max_drawdown, DRAWDOWN_STRESS_THRESHOLD)
        or realized_vol >= VOLATILITY_EXPANSION_ALIGNMENT_THRESHOLD
    ):
        return _alignment("risk_cap_aligned", 1.0, "no_error")
    if realized_vol >= STRESS_REALIZED_VOLATILITY_THRESHOLD * 0.5 or _lte(
        max_drawdown,
        MILD_DRAWDOWN_THRESHOLD,
    ):
        return _alignment("risk_cap_partially_aligned", 0.5, "no_error")
    return _alignment("risk_cap_inconclusive", 0.0, "no_error")


def build_confirmation_only_validation_scorecard(
    active_outcome_rows: Sequence[Mapping[str, Any]],
    inactive_outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in active_outcome_rows
        if str(row.get("usage_role")) == "confirmation_only"
    ]
    inactive = [
        row
        for row in inactive_outcome_rows
        if str(row.get("usage_role")) == "confirmation_only"
    ]
    grouped = _group_by_candidate(rows, default_ids=[CONFIRMATION_CANDIDATE_ID])
    inactive_by_candidate = _group_by_candidate(inactive)
    result: list[dict[str, Any]] = []
    for candidate_id, candidate_rows in grouped.items():
        eligible = _eligible_rows(candidate_rows)
        inactive_eligible = _eligible_rows(inactive_by_candidate.get(candidate_id, []))
        false_confirmations = [
            row for row in eligible if row.get("error_type") == "false_confirmation"
        ]
        false_warnings = [row for row in eligible if row.get("error_type") == "false_warning"]
        active_score = _mean([to_float(row.get("alignment_score")) for row in eligible])
        inactive_score = _mean(
            [to_float(row.get("alignment_score")) for row in inactive_eligible]
        )
        positive_rows = [
            row
            for row in eligible
            if row.get("confirmation_direction") == "positive_confirmation"
        ]
        negative_rows = [
            row
            for row in eligible
            if row.get("confirmation_direction") == "negative_confirmation"
        ]
        asset_scores = _average_by(eligible, "target_asset", "alignment_score")
        horizon_scores = _average_by(eligible, "horizon", "alignment_score")
        status = _confirmation_candidate_status(
            active_eligible_count=len(eligible),
            active_vs_inactive_delta=active_score - inactive_score,
            false_confirmation_cost=_error_cost(false_confirmations),
            false_warning_cost=_error_cost(false_warnings),
            data_quality_blocked=False,
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": "confirmation_only",
                    "active_record_count": len(candidate_rows),
                    "active_eligible_count": len(eligible),
                    "confirmation_alignment_rate": round_float(_alignment_rate(eligible)),
                    "confirmation_weighted_alignment_score": round_float(active_score),
                    "active_vs_inactive_alignment_delta": round_float(
                        active_score - inactive_score
                    ),
                    "trend_confirming_count": sum(
                        1
                        for row in eligible
                        if row.get("confirmation_direction") == "positive_confirmation"
                    ),
                    "trend_weakening_count": sum(
                        1
                        for row in eligible
                        if row.get("confirmation_direction") == "negative_confirmation"
                    ),
                    "false_confirmation_count": len(false_confirmations),
                    "false_warning_count": len(false_warnings),
                    "false_confirmation_cost": round_float(_error_cost(false_confirmations)),
                    "false_warning_cost": round_float(_error_cost(false_warnings)),
                    "average_forward_return_when_positive_confirmation": round_float(
                        _mean([to_float(row.get("actual_forward_return")) for row in positive_rows])
                    ),
                    "average_drawdown_when_negative_confirmation": round_float(
                        _mean([to_float(row.get("actual_max_drawdown")) for row in negative_rows])
                    ),
                    "best_asset": _best_key(asset_scores),
                    "best_horizon": _best_key(horizon_scores),
                    "worst_asset": _worst_key(asset_scores),
                    "worst_horizon": _worst_key(horizon_scores),
                    "recommended_confirmation_status": status,
                    "owner_review_candidate_recommendation": False,
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                    **_boundary_fields(),
                }
            )
        )
    return result


def build_risk_cap_only_validation_scorecard(
    active_outcome_rows: Sequence[Mapping[str, Any]],
    inactive_outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        row for row in active_outcome_rows if str(row.get("usage_role")) == "risk_cap_only"
    ]
    inactive = [
        row
        for row in inactive_outcome_rows
        if str(row.get("usage_role")) == "risk_cap_only"
    ]
    grouped = _group_by_candidate(rows, default_ids=[RISK_CAP_CANDIDATE_ID])
    inactive_by_candidate = _group_by_candidate(inactive)
    result: list[dict[str, Any]] = []
    for candidate_id, candidate_rows in grouped.items():
        eligible = _eligible_rows(candidate_rows)
        inactive_eligible = _eligible_rows(inactive_by_candidate.get(candidate_id, []))
        aligned = [
            row
            for row in eligible
            if str(row.get("alignment_label")) in {"risk_cap_aligned", "risk_cap_partially_aligned"}
        ]
        false_risk_cap = [row for row in eligible if row.get("error_type") == "false_risk_cap"]
        missed_upside = [
            row
            for row in false_risk_cap
            if to_float(row.get("actual_forward_return")) > STRONG_POSITIVE_RETURN_THRESHOLD
        ]
        asset_scores = _average_by(eligible, "target_asset", "alignment_score")
        horizon_scores = _average_by(eligible, "horizon", "alignment_score")
        active_score = _mean([to_float(row.get("alignment_score")) for row in eligible])
        inactive_score = _mean(
            [to_float(row.get("alignment_score")) for row in inactive_eligible]
        )
        status = _risk_cap_candidate_status(
            active_eligible_count=len(eligible),
            active_vs_inactive_delta=active_score - inactive_score,
            false_risk_cap_cost=_error_cost(false_risk_cap),
            missed_upside_cost=_missed_upside_cost(missed_upside),
            data_quality_blocked=False,
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": "risk_cap_only",
                    "active_record_count": len(candidate_rows),
                    "active_eligible_count": len(eligible),
                    "risk_cap_capture_rate": round_float(_ratio(len(aligned), len(eligible))),
                    "stress_event_capture_rate": round_float(
                        _ratio(sum(1 for row in eligible if row.get("stress_event")), len(eligible))
                    ),
                    "downside_tail_capture_rate": round_float(
                        _ratio(
                            sum(1 for row in eligible if row.get("downside_tail_event")),
                            len(eligible),
                        )
                    ),
                    "average_max_drawdown_during_active": round_float(
                        _mean([to_float(row.get("actual_max_drawdown")) for row in eligible])
                    ),
                    "average_forward_return_during_active": round_float(
                        _mean([to_float(row.get("actual_forward_return")) for row in eligible])
                    ),
                    "average_realized_volatility_during_active": round_float(
                        _mean(
                            [to_float(row.get("actual_realized_volatility")) for row in eligible]
                        )
                    ),
                    "false_risk_cap_count": len(false_risk_cap),
                    "false_risk_cap_cost": round_float(_error_cost(false_risk_cap)),
                    "missed_upside_cost": round_float(_missed_upside_cost(missed_upside)),
                    "risk_cap_precision": round_float(_ratio(len(aligned), len(eligible))),
                    "risk_cap_sample_sufficiency": _sample_status_for_counts(
                        active_eligible_count=len(eligible),
                        minimum_active_eligible_records=RISK_CAP_MINIMUM_ACTIVE_ELIGIBLE_RECORDS,
                        sparse_bucket_count=0,
                    ),
                    "best_asset": _best_key(asset_scores),
                    "best_horizon": _best_key(horizon_scores),
                    "worst_asset": _worst_key(asset_scores),
                    "worst_horizon": _worst_key(horizon_scores),
                    "active_vs_inactive_alignment_delta": round_float(
                        active_score - inactive_score
                    ),
                    "recommended_risk_cap_status": status,
                    "owner_review_candidate_recommendation": False,
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                    **_boundary_fields(),
                }
            )
        )
    return result


def build_scope_narrowed_inactive_reference_matrix(
    inactive_outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for candidate_id, rows in _group_by_candidate(inactive_outcome_rows).items():
        eligible = _eligible_rows(rows)
        false_rows = [
            row
            for row in eligible
            if row.get("error_type")
            in {"false_confirmation", "false_warning", "false_risk_cap"}
        ]
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": rows[0].get("usage_role") if rows else "",
                    "inactive_record_count": len(rows),
                    "inactive_eligible_count": len(eligible),
                    "inactive_alignment_reference": round_float(_alignment_rate(eligible)),
                    "inactive_reference_score": round_float(
                        _mean([to_float(row.get("alignment_score")) for row in eligible])
                    ),
                    "inactive_average_forward_return": round_float(
                        _mean([to_float(row.get("actual_forward_return")) for row in eligible])
                    ),
                    "inactive_average_max_drawdown": round_float(
                        _mean([to_float(row.get("actual_max_drawdown")) for row in eligible])
                    ),
                    "inactive_average_realized_volatility": round_float(
                        _mean(
                            [to_float(row.get("actual_realized_volatility")) for row in eligible]
                        )
                    ),
                    "inactive_false_signal_reference": len(false_rows),
                    "inactive_reference_false_cost": round_float(_error_cost(false_rows)),
                    "reference_only": True,
                    **_safety_fields(),
                }
            )
        )
    return result


def build_scope_narrowed_active_vs_inactive_comparison(
    *,
    active_outcome_rows: Sequence[Mapping[str, Any]],
    inactive_outcome_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    sample_rows: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    active_by_candidate = _group_by_candidate(active_outcome_rows)
    inactive_by_candidate = _group_by_candidate(inactive_outcome_rows)
    score_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): row for row in scorecards
    }
    sample_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): row for row in sample_rows
    }
    quality_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): row for row in data_quality_rows
    }
    candidate_ids = sorted(
        set(active_by_candidate) | set(inactive_by_candidate) | set(score_by_candidate)
    )
    result: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        active_rows = active_by_candidate.get(candidate_id, [])
        inactive_rows = inactive_by_candidate.get(candidate_id, [])
        active_eligible = _eligible_rows(active_rows)
        inactive_eligible = _eligible_rows(inactive_rows)
        active_score = _mean(
            [to_float(row.get("alignment_score")) for row in active_eligible]
        )
        inactive_score = _mean(
            [to_float(row.get("alignment_score")) for row in inactive_eligible]
        )
        active_false = [
            row
            for row in active_eligible
            if row.get("error_type")
            in {"false_confirmation", "false_warning", "false_risk_cap"}
        ]
        inactive_false = [
            row
            for row in inactive_eligible
            if row.get("error_type")
            in {"false_confirmation", "false_warning", "false_risk_cap"}
        ]
        usage_role = str(
            (active_rows or inactive_rows or [score_by_candidate.get(candidate_id, {})])[0].get(
                "usage_role"
            )
            or ""
        )
        sample = sample_by_candidate.get(candidate_id, {})
        quality = quality_by_candidate.get(candidate_id, {})
        comparison_label = _active_vs_inactive_label(
            active_eligible_count=len(active_eligible),
            usage_role=usage_role,
            active_vs_inactive_delta=active_score - inactive_score,
            sample_status=str(sample.get("sample_sufficiency_status") or ""),
            data_quality_status=str(quality.get("data_quality_status") or "PASS"),
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": usage_role,
                    "active_record_count": len(active_rows),
                    "inactive_record_count": len(inactive_rows),
                    "active_eligible_count": len(active_eligible),
                    "inactive_eligible_count": len(inactive_eligible),
                    "active_alignment_score": round_float(active_score),
                    "inactive_reference_score": round_float(inactive_score),
                    "active_vs_inactive_score_delta": round_float(
                        active_score - inactive_score
                    ),
                    "active_average_forward_return": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_forward_return"))
                                for row in active_eligible
                            ]
                        )
                    ),
                    "inactive_average_forward_return": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_forward_return"))
                                for row in inactive_eligible
                            ]
                        )
                    ),
                    "active_average_max_drawdown": round_float(
                        _mean([to_float(row.get("actual_max_drawdown")) for row in active_eligible])
                    ),
                    "inactive_average_max_drawdown": round_float(
                        _mean(
                            [to_float(row.get("actual_max_drawdown")) for row in inactive_eligible]
                        )
                    ),
                    "active_average_realized_volatility": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_realized_volatility"))
                                for row in active_eligible
                            ]
                        )
                    ),
                    "inactive_average_realized_volatility": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_realized_volatility"))
                                for row in inactive_eligible
                            ]
                        )
                    ),
                    "active_false_cost": round_float(_error_cost(active_false)),
                    "inactive_reference_false_cost": round_float(_error_cost(inactive_false)),
                    "comparison_label": comparison_label,
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return result


def build_scope_narrowed_sample_sufficiency_report(
    active_outcome_rows: Sequence[Mapping[str, Any]],
    *,
    candidate_ids: Sequence[str],
    usage_by_candidate: Mapping[str, str],
) -> list[dict[str, Any]]:
    active_by_candidate = _group_by_candidate(active_outcome_rows)
    result: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        usage_role = str(usage_by_candidate.get(candidate_id) or "")
        rows = active_by_candidate.get(candidate_id, [])
        eligible = _eligible_rows(rows)
        min_records, min_bucket_records = _sample_thresholds(usage_role)
        asset_horizon_counts = Counter(
            (str(row.get("target_asset")), str(row.get("horizon")))
            for row in eligible
        )
        direction_counts = Counter(str(row.get("signal_direction")) for row in eligible)
        high_counts = Counter(str(_bool(row.get("high_conviction_flag"))) for row in eligible)
        asset_horizon_min = min(asset_horizon_counts.values()) if asset_horizon_counts else 0
        sparse_count = sum(
            1 for count in asset_horizon_counts.values() if count < min_bucket_records
        )
        direction_min = min(direction_counts.values()) if direction_counts else 0
        high_min = min(high_counts.values()) if high_counts else 0
        status = _sample_status_for_counts(
            active_eligible_count=len(eligible),
            minimum_active_eligible_records=min_records,
            sparse_bucket_count=sparse_count,
        )
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": usage_role,
                    "active_record_count": len(rows),
                    "active_eligible_count": len(eligible),
                    "eligible_ratio": round_float(_ratio(len(eligible), len(rows))),
                    "asset_horizon_min_sample": asset_horizon_min,
                    "asset_horizon_sparse_bucket_count": sparse_count,
                    "direction_min_sample": direction_min,
                    "high_conviction_min_sample": high_min,
                    "sample_sufficiency_status": status,
                    "sample_sufficiency_notes": _sample_notes(
                        usage_role=usage_role,
                        active_eligible_count=len(eligible),
                        minimum_active_eligible_records=min_records,
                        sparse_bucket_count=sparse_count,
                        minimum_bucket_records=min_bucket_records,
                    ),
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return result


def build_scope_narrowed_false_signal_cost_matrix(
    active_outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in active_outcome_rows:
        key = (
            str(row.get("scope_narrowed_candidate_id")),
            str(row.get("target_asset")),
            str(row.get("horizon")),
        )
        grouped[key].append(row)
    result: list[dict[str, Any]] = []
    for (candidate_id, target_asset, horizon), rows in sorted(grouped.items()):
        eligible = _eligible_rows(rows)
        false_confirmation = [
            row for row in eligible if row.get("error_type") == "false_confirmation"
        ]
        false_warning = [row for row in eligible if row.get("error_type") == "false_warning"]
        false_risk_cap = [
            row for row in eligible if row.get("error_type") == "false_risk_cap"
        ]
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": rows[0].get("usage_role") if rows else "",
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "eligible_record_count": len(eligible),
                    "false_confirmation_count": len(false_confirmation),
                    "false_warning_count": len(false_warning),
                    "false_risk_cap_count": len(false_risk_cap),
                    "false_confirmation_cost": round_float(_error_cost(false_confirmation)),
                    "false_warning_cost": round_float(_error_cost(false_warning)),
                    "false_risk_cap_cost": round_float(_error_cost(false_risk_cap)),
                    "total_false_signal_cost": round_float(
                        _error_cost(false_confirmation)
                        + _error_cost(false_warning)
                        + _error_cost(false_risk_cap)
                    ),
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return result


def build_scope_narrowed_state_recommendation_matrix(
    *,
    comparison_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    sample_rows: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    score_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): row for row in scorecards
    }
    sample_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): row for row in sample_rows
    }
    quality_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): row for row in data_quality_rows
    }
    result: list[dict[str, Any]] = []
    for comparison in comparison_rows:
        candidate_id = str(comparison.get("scope_narrowed_candidate_id"))
        usage_role = str(comparison.get("usage_role") or "")
        scorecard = score_by_candidate.get(candidate_id, {})
        sample = sample_by_candidate.get(candidate_id, {})
        quality = quality_by_candidate.get(candidate_id, {})
        status = _state_recommendation(
            comparison_label=str(comparison.get("comparison_label") or ""),
            sample_status=str(sample.get("sample_sufficiency_status") or ""),
            data_quality_status=str(quality.get("data_quality_status") or "PASS"),
            active_vs_inactive_delta=to_float(
                comparison.get("active_vs_inactive_score_delta")
            ),
            active_false_cost=to_float(comparison.get("active_false_cost")),
            active_eligible_count=int(comparison.get("active_eligible_count") or 0),
            usage_role=usage_role,
        )
        forward_observe = status == "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE"
        result.append(
            clean_for_yaml(
                {
                    "scope_narrowed_candidate_id": candidate_id,
                    "usage_role": usage_role,
                    "recommended_research_status": status,
                    "usage_specific_status": scorecard.get(
                        "recommended_confirmation_status",
                        scorecard.get("recommended_risk_cap_status", ""),
                    ),
                    "comparison_label": comparison.get("comparison_label"),
                    "sample_sufficiency_status": sample.get("sample_sufficiency_status"),
                    "data_quality_status": quality.get("data_quality_status"),
                    "forward_observe_candidate_recommendation": forward_observe,
                    "owner_review_candidate_recommendation": False,
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                    **_recommendation_safety_fields(),
                    **_boundary_fields(),
                }
            )
        )
    for row in result:
        status = str(row.get("recommended_research_status") or "")
        if status not in ALLOWED_STATE_RECOMMENDATIONS:
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"invalid scope-narrowed state recommendation: {status}"
            )
    return result


def build_scope_narrowed_data_quality_row(
    *,
    scope_narrowed_candidate_id: str,
    usage_role: str,
    actual_path_rows: Sequence[Mapping[str, Any]],
    input_artifact_validation_status: str,
) -> dict[str, Any]:
    status_counts = Counter(str(row.get("actual_path_status")) for row in actual_path_rows)
    missing_target_asset_count = status_counts["missing_target_asset"]
    warning_count = (
        status_counts["missing_decision_price"]
        + status_counts["incomplete_future_window"]
        + status_counts["partial_price_coverage"]
    )
    error_count = missing_target_asset_count
    data_quality_status = (
        "FAIL"
        if error_count > 0 or input_artifact_validation_status != "PASS"
        else "PASS_WITH_WARNINGS"
        if warning_count > 0
        else "PASS"
    )
    return clean_for_yaml(
        {
            "scope_narrowed_candidate_id": scope_narrowed_candidate_id,
            "usage_role": usage_role,
            "input_artifact_validation_status": input_artifact_validation_status,
            "price_data_coverage_status": data_quality_status,
            "missing_decision_price_count": status_counts["missing_decision_price"],
            "partial_future_window_count": status_counts["partial_price_coverage"],
            "incomplete_horizon_count": status_counts["incomplete_future_window"],
            "missing_target_asset_count": missing_target_asset_count,
            "data_quality_warning_count": warning_count,
            "data_quality_error_count": error_count,
            "data_quality_status": data_quality_status,
            **_safety_fields(),
            **_recommendation_safety_fields(),
        }
    )


def scope_narrowed_error_attribution_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "scope_narrowed_candidate_id": row.get("scope_narrowed_candidate_id"),
            "usage_role": row.get("usage_role"),
            "record_id": row.get("record_id"),
            "target_asset": row.get("target_asset"),
            "decision_timestamp": row.get("decision_timestamp"),
            "horizon": row.get("horizon"),
            "signal_direction": row.get("signal_direction"),
            "scope_active": row.get("scope_active"),
            "actual_forward_return": row.get("actual_forward_return"),
            "actual_max_drawdown": row.get("actual_max_drawdown"),
            "actual_realized_volatility": row.get("actual_realized_volatility"),
            "alignment_label": row.get("alignment_label"),
            "error_type": _normalized_error_type(row),
            "dominant_observed_driver": row.get("dominant_observed_driver"),
            "owner_review_note": _owner_review_note(row),
            **_safety_fields(),
            **_recommendation_safety_fields(),
        }
    )


def build_risk_appetite_archive_carry_forward(
    archive_record: Mapping[str, Any],
) -> dict[str, Any]:
    if archive_record.get("candidate_id") != RISK_APPETITE_ARCHIVE_CANDIDATE:
        raise ScopeNarrowedCandidateActualPathValidationError(
            "risk_appetite archive carry-forward requires risk_appetite current form"
        )
    return clean_for_yaml(
        {
            "schema_version": "risk_appetite_archive_carry_forward.v1",
            "report_type": "scope_narrowed_candidate_actual_path_validation",
            "artifact_role": ARTIFACT_ROLE,
            "task_id": TASK_ID,
            "candidate_id": RISK_APPETITE_ARCHIVE_CANDIDATE,
            "archive_source_task": INPUT_SCOPE_NARROWING_TASK,
            "archive_status": "current_form_archived",
            "included_in_scope_narrowed_validation": False,
            "future_reopen_policy": {
                "policy": "reopen_only_with_new_inputs_or_candidate_family",
                "current_form_validation_allowed": False,
            },
            **_safety_fields(),
            **_recommendation_safety_fields(),
            **_boundary_fields(),
            "trading_2285_original_inconclusive_decisions_changed": False,
            "trading_2289_refined_state_decisions_changed": False,
            "trading_2291_scope_narrowing_decisions_changed": False,
        }
    )


def _build_output_payloads(
    *,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    active_actual_rows: Sequence[Mapping[str, Any]],
    active_outcome_rows: Sequence[Mapping[str, Any]],
    inactive_reference_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
    confirmation_scorecards: Sequence[Mapping[str, Any]],
    risk_cap_scorecards: Sequence[Mapping[str, Any]],
    sample_rows: Sequence[Mapping[str, Any]],
    false_cost_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    error_rows: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
    archive_carry_forward: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        "summary": {**common, "summary": summary},
        "active_actual_path": {**common, "rows": list(active_actual_rows)},
        "active_outcome": {**common, "rows": list(active_outcome_rows)},
        "inactive_reference": {**common, "rows": list(inactive_reference_rows)},
        "comparison": {**common, "rows": list(comparison_rows)},
        "confirmation_scorecard": {
            **common,
            "candidate_scorecards": list(confirmation_scorecards),
        },
        "risk_cap_scorecard": {
            **common,
            "candidate_scorecards": list(risk_cap_scorecards),
        },
        "sample_sufficiency": {**common, "rows": list(sample_rows)},
        "false_signal_cost": {**common, "rows": list(false_cost_rows)},
        "state_recommendation": {**common, "candidate_rows": list(state_rows)},
        "error_attribution_seed": {**common, "error_rows": list(error_rows)},
        "data_quality_report": {**common, "candidate_rows": list(data_quality_rows)},
        "risk_appetite_archive_carry_forward": dict(archive_carry_forward),
    }


def _summary(
    *,
    include_candidates: Sequence[str],
    archived_candidates: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
    active_actual_rows: Sequence[Mapping[str, Any]],
    active_outcome_rows: Sequence[Mapping[str, Any]],
    inactive_outcome_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    source_data_quality: Mapping[str, Any],
    scope_narrowed_generator_dir: Path,
    scope_review_dir: Path,
    refined_validation_dir: Path,
    prices_path: Path,
) -> dict[str, Any]:
    eligible_active = sum(1 for row in active_actual_rows if bool(row.get("validation_eligible")))
    forward_count = sum(
        1 for row in state_rows if bool(row.get("forward_observe_candidate_recommendation"))
    )
    state_by_candidate = {
        str(row.get("scope_narrowed_candidate_id")): str(
            row.get("recommended_research_status")
        )
        for row in state_rows
    }
    output_data_quality_status = (
        "FAIL"
        if any(row.get("data_quality_status") == "FAIL" for row in data_quality_rows)
        else "PASS_WITH_WARNINGS"
        if any(row.get("data_quality_status") == "PASS_WITH_WARNINGS" for row in data_quality_rows)
        else "PASS"
    )
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": "ai_after_chatgpt",
            "requested_date_range": "scope_narrowed_artifact_decision_timestamps",
            "input_scope_narrowing_task": INPUT_SCOPE_NARROWING_TASK,
            "scope_narrowed_generator_dir": str(scope_narrowed_generator_dir),
            "scope_review_dir": str(scope_review_dir),
            "refined_validation_dir": str(refined_validation_dir),
            "prices_path": str(prices_path),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "candidate_count": len(include_candidates),
            "validated_candidate_ids": list(include_candidates),
            "archived_candidate_ids": list(archived_candidates),
            "active_record_count_total": len(active_actual_rows),
            "eligible_active_record_count_total": eligible_active,
            "inactive_reference_record_count_total": len(inactive_outcome_rows),
            "confirmation_candidate_status": state_by_candidate.get(
                CONFIRMATION_CANDIDATE_ID,
                "SCOPE_NARROWED_VALIDATED_INCONCLUSIVE",
            ),
            "risk_cap_candidate_status": state_by_candidate.get(
                RISK_CAP_CANDIDATE_ID,
                "SCOPE_NARROWED_VALIDATED_INCONCLUSIVE",
            ),
            "forward_observe_candidate_count": forward_count,
            "owner_review_candidate_count": 0,
            "data_quality_status": output_data_quality_status,
            "source_data_quality_status": source_data_quality.get("status"),
            "source_data_quality_error_count": source_data_quality.get("error_count"),
            "source_data_quality_warning_count": source_data_quality.get("warning_count"),
            "comparison_labels": Counter(
                str(row.get("comparison_label")) for row in comparison_rows
            ),
            "candidate_recommendation_counts": Counter(
                str(row.get("recommended_research_status")) for row in state_rows
            ),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "owner_review_required": False,
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            "broker_action_recommendation_allowed": False,
            "next_task_recommendation": _next_task_recommendation(state_rows),
            "artifact_role": ARTIFACT_ROLE,
            **_boundary_fields(),
            "trading_2285_original_inconclusive_decisions_changed": False,
            "trading_2289_refined_state_decisions_changed": False,
            "trading_2291_scope_narrowing_decisions_changed": False,
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    summary: Mapping[str, Any],
    mode: str,
) -> dict[str, Any]:
    return {
        "schema_version": "scope_narrowed_candidate_actual_path_validation.v1",
        "report_type": "scope_narrowed_candidate_actual_path_validation",
        "artifact_role": ARTIFACT_ROLE,
        "title": "Scope-Narrowed Candidate Actual-Path Validation",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "research_only": True,
        "summary_status": summary.get("status"),
        "owner_review_required": False,
        **_safety_fields(),
        **_recommendation_safety_fields(),
        **_boundary_fields(),
        "trading_2285_original_inconclusive_decisions_changed": False,
        "trading_2289_refined_state_decisions_changed": False,
        "trading_2291_scope_narrowing_decisions_changed": False,
    }


def _artifact_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "scope_narrowed_actual_path_validation_summary.json",
        "active_actual_path_json": output_dir / "scope_narrowed_active_actual_path_matrix.json",
        "active_actual_path_csv": output_dir / "scope_narrowed_active_actual_path_matrix.csv",
        "active_outcome_json": (
            output_dir / "scope_narrowed_active_prediction_outcome_matrix.json"
        ),
        "active_outcome_csv": (
            output_dir / "scope_narrowed_active_prediction_outcome_matrix.csv"
        ),
        "inactive_reference_json": output_dir / "scope_narrowed_inactive_reference_matrix.json",
        "inactive_reference_csv": output_dir / "scope_narrowed_inactive_reference_matrix.csv",
        "comparison_json": output_dir / "scope_narrowed_active_vs_inactive_comparison.json",
        "comparison_csv": output_dir / "scope_narrowed_active_vs_inactive_comparison.csv",
        "confirmation_scorecard_json": output_dir / "confirmation_only_validation_scorecard.json",
        "confirmation_scorecard_csv": output_dir / "confirmation_only_validation_scorecard.csv",
        "risk_cap_scorecard_json": output_dir / "risk_cap_only_validation_scorecard.json",
        "risk_cap_scorecard_csv": output_dir / "risk_cap_only_validation_scorecard.csv",
        "sample_sufficiency_json": (
            output_dir / "scope_narrowed_sample_sufficiency_report.json"
        ),
        "sample_sufficiency_csv": (
            output_dir / "scope_narrowed_sample_sufficiency_report.csv"
        ),
        "false_signal_cost_json": output_dir / "scope_narrowed_false_signal_cost_matrix.json",
        "false_signal_cost_csv": output_dir / "scope_narrowed_false_signal_cost_matrix.csv",
        "state_recommendation_json": output_dir / "scope_narrowed_state_recommendation_matrix.json",
        "state_recommendation_csv": output_dir / "scope_narrowed_state_recommendation_matrix.csv",
        "error_attribution_seed": output_dir / "scope_narrowed_error_attribution_seed.json",
        "data_quality_report": output_dir / "scope_narrowed_data_quality_report.json",
        "risk_appetite_archive_carry_forward": (
            output_dir / "risk_appetite_archive_carry_forward.json"
        ),
        "validation_report_doc": (
            docs_root / "scope_narrowed_candidate_actual_path_validation_report.md"
        ),
        "confirmation_doc": docs_root / "confirmation_only_actual_path_validation.md",
        "risk_cap_doc": docs_root / "risk_cap_only_actual_path_validation.md",
        "comparison_doc": docs_root / "scope_narrowed_active_vs_inactive_comparison.md",
        "state_doc": docs_root / "scope_narrowed_state_recommendation.md",
    }


def _validate_requested_candidates(
    include_ids: Sequence[str],
    archive_ids: Sequence[str],
) -> None:
    if RISK_APPETITE_ARCHIVE_CANDIDATE not in set(archive_ids):
        raise ScopeNarrowedCandidateActualPathValidationError(
            "risk_appetite_refined_confidence_v1 archive candidate is required"
        )
    if RISK_APPETITE_ARCHIVE_CANDIDATE in set(include_ids):
        raise ScopeNarrowedCandidateActualPathValidationError(
            "risk_appetite_refined_confidence_v1 cannot be included in validation"
        )
    for candidate_id in include_ids:
        if candidate_id not in USAGE_BY_CANDIDATE:
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"unsupported scope-narrowed candidate: {candidate_id}"
            )


def _validate_top_level_state(
    payloads: Mapping[str, Mapping[str, Any]],
    include_ids: Sequence[str],
) -> None:
    validation = payloads["validation_summary"]
    if str(validation.get("status")) != "PASS":
        raise ScopeNarrowedCandidateActualPathValidationError(
            "scope-narrowed regeneration validation summary status must be PASS"
        )
    registry_rows = _rows_from_payload(payloads["registry"], "rows")
    registry_ids = {str(row.get("scope_narrowed_candidate_id")) for row in registry_rows}
    missing = set(include_ids) - registry_ids
    if missing:
        raise ScopeNarrowedCandidateActualPathValidationError(
            f"scope-narrowed registry missing included candidates: {sorted(missing)}"
        )


def _validate_scope_review_context(
    payloads: Mapping[str, Mapping[str, Any]],
    include_ids: Sequence[str],
) -> None:
    rows = _rows_from_payload(payloads["scope_recommendation"], "rows")
    refined_ids = {_refined_id_for_scope_candidate(candidate_id) for candidate_id in include_ids}
    by_refined = {str(row.get("refined_candidate_id")): row for row in rows}
    missing = refined_ids - set(by_refined)
    if missing:
        raise ScopeNarrowedCandidateActualPathValidationError(
            f"scope review missing refined candidates: {sorted(missing)}"
        )
    for refined_id in refined_ids:
        row = by_refined[refined_id]
        if str(row.get("recommended_scope_action")) != "SCOPE_NARROW_AND_REGENERATE":
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{refined_id}: scope review is not SCOPE_NARROW_AND_REGENERATE"
            )


def _validate_refined_validation_context(
    payloads: Mapping[str, Mapping[str, Any]],
    include_ids: Sequence[str],
) -> None:
    state_rows = _rows_from_payload(payloads["state"], "candidate_rows")
    by_refined = {str(row.get("refined_candidate_id")): row for row in state_rows}
    for candidate_id in include_ids:
        refined_id = _refined_id_for_scope_candidate(candidate_id)
        row = by_refined.get(refined_id)
        if not row:
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{refined_id}: missing refined validation state row"
            )
        if str(row.get("recommended_research_status")) != (
            "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"
        ):
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{refined_id}: refined validation state is not continue research"
            )


def _validate_risk_appetite_archive_record(
    archive_record: Mapping[str, Any],
    archive_ids: Sequence[str],
) -> None:
    if archive_record.get("candidate_id") != RISK_APPETITE_ARCHIVE_CANDIDATE:
        raise ScopeNarrowedCandidateActualPathValidationError(
            "missing risk_appetite current-form archive record"
        )
    if RISK_APPETITE_ARCHIVE_CANDIDATE not in set(archive_ids):
        raise ScopeNarrowedCandidateActualPathValidationError(
            "risk_appetite archive carry-forward not requested"
        )


def _scope_required_field_errors(
    scope: str,
    payload: Mapping[str, Any],
    *,
    usage_role: str,
    require_scope_fields: bool,
) -> list[str]:
    required = [
        "candidate_id",
        "scope_narrowed_candidate_id",
        "refined_candidate_id",
        "original_candidate_id",
        "source_artifact_hash",
        "source_refined_artifact_hash",
        "source_scope_review_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "valid_from",
        "valid_until",
        "horizon",
        "provenance",
        "usage_role",
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "broker_action",
    ]
    if require_scope_fields:
        required.append("scope_active")
    errors: list[str] = []
    for field in required:
        if _is_missing(payload.get(field)):
            errors.append(f"{scope}: missing {field}")
    if payload.get("usage_role") != usage_role:
        errors.append(f"{scope}: usage_role mismatch")
    if _bool(payload.get("promotion_allowed")) is not False:
        errors.append(f"{scope}: promotion_allowed must be false")
    if _bool(payload.get("paper_shadow_allowed")) is not False:
        errors.append(f"{scope}: paper_shadow_allowed must be false")
    if _bool(payload.get("production_allowed")) is not False:
        errors.append(f"{scope}: production_allowed must be false")
    if str(payload.get("broker_action") or "") != "none":
        errors.append(f"{scope}: broker_action must be none")
    if _bool(payload.get("actual_path_validation_ready")):
        errors.append(f"{scope}: actual_path_validation_ready must be false")
    if _bool(payload.get("owner_review_required")):
        errors.append(f"{scope}: owner_review_required must be false")
    return errors


def _load_json_required(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        raise ScopeNarrowedCandidateActualPathValidationError(f"missing {label}: {path}")
    payload = _read_json_object(path)
    _assert_input_payload_safe(label, payload)
    return payload


def _read_json_object(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        raise ScopeNarrowedCandidateActualPathValidationError(
            f"JSON payload must be an object: {path}"
        )
    return dict(payload)


def _read_signal_series_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = []
        for row in csv.DictReader(handle):
            parsed = dict(row)
            for field in (
                "provenance",
                "source_prediction_flags",
                "inactive_reasons",
                "selected_proposal_ids",
                "selected_parameter_set_ids",
            ):
                if isinstance(parsed.get(field), str) and str(parsed[field]).strip():
                    try:
                        parsed[field] = json.loads(str(parsed[field]))
                    except json.JSONDecodeError:
                        pass
            rows.append(parsed)
    return rows


def _assert_input_payload_safe(scope: str, payload: Any) -> None:
    for path, item in _walk_payload(payload):
        label = f"{scope}{path}"
        if isinstance(item, Mapping):
            for field in ("promotion_allowed", "paper_shadow_allowed", "production_allowed"):
                if field in item and _bool(item.get(field)) is not False:
                    raise ScopeNarrowedCandidateActualPathValidationError(
                        f"{label}: {field} must be false"
                    )
            if "broker_action" in item and str(item.get("broker_action") or "") != "none":
                raise ScopeNarrowedCandidateActualPathValidationError(
                    f"{label}: broker_action must be none"
                )
            if "actual_path_validation_ready" in item and _bool(
                item.get("actual_path_validation_ready")
            ):
                raise ScopeNarrowedCandidateActualPathValidationError(
                    f"{label}: actual_path_validation_ready must be false"
                )
            if "owner_review_required" in item and _bool(item.get("owner_review_required")):
                raise ScopeNarrowedCandidateActualPathValidationError(
                    f"{label}: owner_review_required must be false"
                )
        if isinstance(item, str) and item in BANNED_RECOMMENDATIONS:
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{label}: banned readiness recommendation {item}"
            )


def _assert_generated_payload_safe(scope: str, payload: Any) -> None:
    _assert_input_payload_safe(scope, payload)
    for path, item in _walk_payload(payload):
        key = path.rsplit(".", maxsplit=1)[-1]
        if key in {
            "paper_shadow_recommendation_allowed",
            "production_recommendation_allowed",
            "broker_action_recommendation_allowed",
            "owner_review_required",
        } and _bool(item):
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{scope}{path}: {key} must be false"
            )
        if key == "broker_action" and str(item or "") != "none":
            raise ScopeNarrowedCandidateActualPathValidationError(
                f"{scope}{path}: broker_action must be none"
            )


def _scope_narrowed_prediction_record(
    record: Mapping[str, Any],
    bundle: ScopeNarrowedCandidateArtifacts,
) -> dict[str, Any]:
    row = dict(record)
    row.setdefault("candidate_id", bundle.scope_narrowed_candidate_id)
    row.setdefault("scope_narrowed_candidate_id", bundle.scope_narrowed_candidate_id)
    row.setdefault("refined_candidate_id", bundle.refined_candidate_id)
    row.setdefault("original_candidate_id", bundle.original_candidate_id)
    row.setdefault("usage_role", bundle.usage_role)
    if row.get("signal_confidence") in (None, ""):
        row["signal_confidence"] = row.get("confirmation_confidence") or row.get(
            "risk_cap_score"
        )
    return row


def _is_active_validation_record(record: Mapping[str, Any]) -> bool:
    if not _bool(record.get("scope_active")):
        return False
    usage_role = str(record.get("usage_role") or "")
    direction = str(record.get("signal_direction") or "")
    if usage_role == "risk_cap_only":
        return direction in RISK_CAP_VALIDATION_DIRECTIONS
    if usage_role == "confirmation_only":
        return direction in CONFIRMATION_VALIDATION_DIRECTIONS
    return False


def _confirmation_direction(record: Mapping[str, Any]) -> str:
    value = str(record.get("confirmation_direction") or "")
    if value in {"positive_confirmation", "negative_confirmation"}:
        return value
    direction = str(record.get("signal_direction") or "")
    if direction in {"trend_confirming", "risk_on"}:
        return "positive_confirmation"
    if direction in {"trend_weakening", "risk_off"}:
        return "negative_confirmation"
    return value or "inactive"


def _confirmation_candidate_status(
    *,
    active_eligible_count: int,
    active_vs_inactive_delta: float,
    false_confirmation_cost: float,
    false_warning_cost: float,
    data_quality_blocked: bool,
) -> str:
    if data_quality_blocked:
        return "CONFIRMATION_SCOPE_DATA_QUALITY_BLOCKED"
    if active_eligible_count == 0:
        return "CONFIRMATION_SCOPE_DATA_QUALITY_BLOCKED"
    if active_eligible_count < CONFIRMATION_MINIMUM_ACTIVE_ELIGIBLE_RECORDS * 0.5:
        return "CONFIRMATION_SCOPE_SAMPLE_BLOCKED"
    false_cost_rate = (false_confirmation_cost + false_warning_cost) / max(
        active_eligible_count,
        1,
    )
    if false_cost_rate > FALSE_COST_ACCEPTABLE_PER_RECORD:
        return "CONFIRMATION_SCOPE_VALIDATED_REJECT_RECOMMENDED"
    if active_vs_inactive_delta >= FORWARD_OBSERVE_MINIMUM_ALIGNMENT_DELTA:
        return "CONFIRMATION_SCOPE_VALIDATED_LOCAL_EDGE"
    if active_vs_inactive_delta < 0.0:
        return "CONFIRMATION_SCOPE_VALIDATED_REJECT_RECOMMENDED"
    return "CONFIRMATION_SCOPE_VALIDATED_CONTINUE_RESEARCH"


def _risk_cap_candidate_status(
    *,
    active_eligible_count: int,
    active_vs_inactive_delta: float,
    false_risk_cap_cost: float,
    missed_upside_cost: float,
    data_quality_blocked: bool,
) -> str:
    if data_quality_blocked:
        return "RISK_CAP_SCOPE_DATA_QUALITY_BLOCKED"
    if active_eligible_count == 0:
        return "RISK_CAP_SCOPE_DATA_QUALITY_BLOCKED"
    if active_eligible_count < RISK_CAP_MINIMUM_ACTIVE_ELIGIBLE_RECORDS * 0.5:
        return "RISK_CAP_SCOPE_SAMPLE_BLOCKED"
    false_cost_rate = (false_risk_cap_cost + missed_upside_cost) / max(
        active_eligible_count,
        1,
    )
    if false_cost_rate > FALSE_COST_ACCEPTABLE_PER_RECORD:
        return "RISK_CAP_SCOPE_VALIDATED_REJECT_RECOMMENDED"
    if active_vs_inactive_delta >= FORWARD_OBSERVE_MINIMUM_ALIGNMENT_DELTA:
        return "RISK_CAP_SCOPE_VALIDATED_LOCAL_EDGE"
    if active_vs_inactive_delta < 0.0:
        return "RISK_CAP_SCOPE_VALIDATED_REJECT_RECOMMENDED"
    return "RISK_CAP_SCOPE_VALIDATED_CONTINUE_RESEARCH"


def _active_vs_inactive_label(
    *,
    active_eligible_count: int,
    usage_role: str,
    active_vs_inactive_delta: float,
    sample_status: str,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "DATA_QUALITY_BLOCKED"
    if sample_status == "SAMPLE_BLOCKED" or active_eligible_count == 0:
        return "ACTIVE_SCOPE_INSUFFICIENT_SAMPLE"
    if active_vs_inactive_delta >= ACTIVE_VS_INACTIVE_STRONG_DELTA:
        return "ACTIVE_SCOPE_OUTPERFORMS_REFERENCE"
    if active_vs_inactive_delta > ACTIVE_VS_INACTIVE_WEAK_DELTA:
        return "ACTIVE_SCOPE_WEAKLY_BETTER"
    if abs(active_vs_inactive_delta) < 0.01:
        return "ACTIVE_SCOPE_NO_MEASURABLE_EDGE"
    if active_vs_inactive_delta < 0.0:
        return "ACTIVE_SCOPE_WORSE"
    if usage_role:
        return "ACTIVE_SCOPE_NO_MEASURABLE_EDGE"
    return "ACTIVE_SCOPE_INSUFFICIENT_SAMPLE"


def _state_recommendation(
    *,
    comparison_label: str,
    sample_status: str,
    data_quality_status: str,
    active_vs_inactive_delta: float,
    active_false_cost: float,
    active_eligible_count: int,
    usage_role: str,
) -> str:
    if data_quality_status == "FAIL" or comparison_label == "DATA_QUALITY_BLOCKED":
        return "SCOPE_NARROWED_DATA_QUALITY_BLOCKED"
    if sample_status == "SAMPLE_BLOCKED" or active_eligible_count == 0:
        return "SCOPE_NARROWED_SAMPLE_BLOCKED"
    false_cost_rate = active_false_cost / max(active_eligible_count, 1)
    if comparison_label == "ACTIVE_SCOPE_WORSE" or false_cost_rate > (
        FALSE_COST_ACCEPTABLE_PER_RECORD * 1.5
    ):
        return "SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED"
    if sample_status in {"SAMPLE_THIN_BUT_USABLE", "SAMPLE_INSUFFICIENT_FOR_SUBGROUPS"}:
        return "SCOPE_NARROWED_VALIDATED_CONTINUE_RESEARCH"
    if (
        comparison_label
        in {"ACTIVE_SCOPE_OUTPERFORMS_REFERENCE", "ACTIVE_SCOPE_WEAKLY_BETTER"}
        and false_cost_rate <= FALSE_COST_ACCEPTABLE_PER_RECORD
        and sample_status == "SAMPLE_SUFFICIENT"
        and usage_role in {"confirmation_only", "risk_cap_only"}
        and active_vs_inactive_delta >= FORWARD_OBSERVE_MINIMUM_ALIGNMENT_DELTA
    ):
        return "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE"
    if (
        comparison_label
        in {"ACTIVE_SCOPE_OUTPERFORMS_REFERENCE", "ACTIVE_SCOPE_WEAKLY_BETTER"}
        and active_vs_inactive_delta >= LOCAL_EDGE_MINIMUM_ALIGNMENT_DELTA
    ):
        return "SCOPE_NARROWED_VALIDATED_LOCAL_EDGE"
    return "SCOPE_NARROWED_VALIDATED_INCONCLUSIVE"


def _sample_thresholds(usage_role: str) -> tuple[int, int]:
    if usage_role == "risk_cap_only":
        return (
            RISK_CAP_MINIMUM_ACTIVE_ELIGIBLE_RECORDS,
            RISK_CAP_MINIMUM_ASSET_HORIZON_BUCKET_RECORDS,
        )
    return (
        CONFIRMATION_MINIMUM_ACTIVE_ELIGIBLE_RECORDS,
        CONFIRMATION_MINIMUM_ASSET_HORIZON_BUCKET_RECORDS,
    )


def _sample_status_for_counts(
    *,
    active_eligible_count: int,
    minimum_active_eligible_records: int,
    sparse_bucket_count: int,
) -> str:
    if active_eligible_count < minimum_active_eligible_records * 0.5:
        return "SAMPLE_BLOCKED"
    if active_eligible_count < minimum_active_eligible_records:
        return "SAMPLE_THIN_BUT_USABLE"
    if sparse_bucket_count > 0:
        return "SAMPLE_INSUFFICIENT_FOR_SUBGROUPS"
    return "SAMPLE_SUFFICIENT"


def _sample_notes(
    *,
    usage_role: str,
    active_eligible_count: int,
    minimum_active_eligible_records: int,
    sparse_bucket_count: int,
    minimum_bucket_records: int,
) -> str:
    if active_eligible_count < minimum_active_eligible_records * 0.5:
        return (
            f"{usage_role} active eligible sample is below blocking floor; "
            "do not infer local edge."
        )
    if active_eligible_count < minimum_active_eligible_records:
        return (
            f"{usage_role} active eligible sample is thin but usable only for "
            "overall continue-research evidence."
        )
    if sparse_bucket_count > 0:
        return (
            f"{usage_role} overall sample passes, but {sparse_bucket_count} "
            f"asset/horizon buckets are below {minimum_bucket_records}."
        )
    return f"{usage_role} active eligible sample passes overall and bucket floors."


def _next_task_recommendation(state_rows: Sequence[Mapping[str, Any]]) -> str:
    statuses = {str(row.get("recommended_research_status")) for row in state_rows}
    if "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE" in statuses:
        return "TRADING-2293_Scope_Narrowed_Forward_Observe_Readiness_Review"
    if "SCOPE_NARROWED_VALIDATED_LOCAL_EDGE" in statuses:
        return "TRADING-2293_Scope_Narrowed_Candidate_Owner_Review_Precheck"
    if statuses and statuses <= {"SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED"}:
        return "TRADING-2293_Archive_Scope_Narrowed_Candidates"
    return "TRADING-2293_Candidate_Family_Redesign_Plan"


def _normalized_error_type(row: Mapping[str, Any]) -> str:
    value = str(row.get("error_type") or "no_error")
    allowed = {
        "false_confirmation",
        "false_warning",
        "false_risk_cap",
        "missed_stress",
        "missed_downside_tail",
        "missed_upside",
        "volatility_misclassification",
        "data_quality_inconclusive",
        "no_error",
    }
    return value if value in allowed else "no_error"


def _dominant_observed_driver(actual_path: Mapping[str, Any]) -> str:
    if not bool(actual_path.get("validation_eligible")):
        return "data_gap"
    forward_return = to_float(actual_path.get("forward_return"))
    max_drawdown = to_float(actual_path.get("max_drawdown_during_horizon"))
    realized_vol = to_float(actual_path.get("realized_volatility"))
    if forward_return >= UPSIDE_BREAKOUT_RETURN_THRESHOLD:
        return "sharp_rebound"
    if max_drawdown <= DOWNSIDE_TAIL_DRAWDOWN_THRESHOLD:
        return "drawdown_event"
    if realized_vol >= VOLATILITY_EXPANSION_ALIGNMENT_THRESHOLD:
        return "volatility_expansion"
    if realized_vol <= 0.18:
        return "volatility_compression"
    if forward_return > POSITIVE_RETURN_THRESHOLD:
        return "trend_continuation"
    if forward_return < NEGATIVE_RETURN_THRESHOLD:
        return "trend_reversal"
    return "noisy_path"


def _owner_review_note(row: Mapping[str, Any]) -> str:
    error_type = _normalized_error_type(row)
    if error_type == "no_error":
        return "No initial scope-narrowed error attribution required."
    if error_type == "data_quality_inconclusive":
        return "Actual-path evidence is ineligible or coverage-limited; review data first."
    return f"Initial TRADING-2292 seed classification: {error_type}."


def _record_id(record: Mapping[str, Any]) -> str:
    existing = record.get("record_id")
    if existing:
        return str(existing)
    return "|".join(
        [
            str(record.get("scope_narrowed_candidate_id") or record.get("candidate_id")),
            str(record.get("target_asset")),
            str(record.get("horizon")),
            str(record.get("signal_direction")),
            str(record.get("decision_timestamp")),
            str(record.get("source_row_index", "")),
        ]
    )


def _alignment(label: str, score: float, error_type: str) -> dict[str, Any]:
    return {
        "alignment_label": label,
        "alignment_score": score,
        "error_type": error_type,
    }


def _refined_id_for_scope_candidate(candidate_id: str) -> str:
    if candidate_id == CONFIRMATION_CANDIDATE_ID:
        return "baseline_plus_trend_structure_refined_confidence_v1"
    if candidate_id == RISK_CAP_CANDIDATE_ID:
        return "volatility_regime_refined_confidence_v1"
    return ""


def _group_by_candidate(
    rows: Sequence[Mapping[str, Any]],
    *,
    default_ids: Sequence[str] = (),
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {candidate_id: [] for candidate_id in default_ids}
    for row in rows:
        grouped.setdefault(str(row.get("scope_narrowed_candidate_id")), []).append(row)
    return grouped


def _candidate_rows(
    rows: Sequence[Mapping[str, Any]],
    candidate_id: str,
) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if str(row.get("scope_narrowed_candidate_id") or row.get("candidate_id"))
        == candidate_id
    ]


def _eligible_rows(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [row for row in rows if bool(row.get("validation_eligible"))]


def _alignment_rate(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    return sum(1 for row in rows if to_float(row.get("alignment_score")) > 0.0) / len(rows)


def _error_cost(rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        abs(to_float(row.get("actual_forward_return")))
        + abs(to_float(row.get("actual_max_drawdown")))
        for row in rows
    )


def _missed_upside_cost(rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(max(to_float(row.get("actual_forward_return")), 0.0) for row in rows)


def _average_by(
    rows: Sequence[Mapping[str, Any]],
    group_key: str,
    value_key: str,
) -> dict[str, float]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = to_float(row.get(value_key), default=float("nan"))
        if math.isfinite(value):
            grouped[str(row.get(group_key))].append(value)
    return {
        key: round_float(sum(values) / len(values))
        for key, values in grouped.items()
        if values
    }


def _best_key(values: Mapping[str, float]) -> str:
    return max(values, key=values.get) if values else ""


def _worst_key(values: Mapping[str, float]) -> str:
    return min(values, key=values.get) if values else ""


def _mean(values: Sequence[float]) -> float:
    clean = [value for value in values if math.isfinite(value)]
    return sum(clean) / len(clean) if clean else 0.0


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    rows = payload.get(key)
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _validator_record_sample(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    parsed = [dict(row) for row in records]
    if len(parsed) <= VALIDATOR_TRACE_SAMPLE_LIMIT:
        return parsed
    head_count = VALIDATOR_TRACE_SAMPLE_LIMIT // 2
    tail_count = VALIDATOR_TRACE_SAMPLE_LIMIT - head_count
    sample = parsed[:head_count] + parsed[-tail_count:]
    active = [row for row in parsed if _bool(row.get("scope_active"))]
    for row in active[:VALIDATOR_TRACE_SAMPLE_LIMIT]:
        if row not in sample:
            sample.append(row)
    return sample


def _safety_fields() -> dict[str, Any]:
    return {
        **candidate_artifact_safety_fields(),
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
    }


def _recommendation_safety_fields() -> dict[str, Any]:
    return {
        "broker_action_recommendation_allowed": False,
    }


def _boundary_fields() -> dict[str, Any]:
    return {
        **trading_2281_boundary_fields(),
    }


def _walk_payload(value: Any, path: str = ""):
    yield path, value
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield from _walk_payload(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_payload(item, f"{path}[{index}]")


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    parsed = tuple(str(item).strip() for item in value if str(item).strip())
    if not parsed:
        raise ScopeNarrowedCandidateActualPathValidationError("input list must be non-empty")
    return parsed


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == []


def _lt(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed < right


def _lte(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed <= right


def _gt(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed > right


def _gte(left: Any, right: float) -> bool:
    parsed = _optional_float(left)
    return parsed is not None and parsed >= right


def _render_validation_report(
    summary: Mapping[str, Any],
    comparison_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Scope-Narrowed Candidate Actual-Path Validation Report",
        "",
        "最后更新：2026-06-30",
        "",
        "TRADING-2292 读取 TRADING-2291 scope-narrowed artifacts，只验证 "
        "`scope_active=true` active records；inactive records 仅作为 reference。",
        "",
        "TRADING-2291 已生成 `baseline_plus_trend_structure_scope_narrowed_confirmation_v1` "
        "和 `volatility_regime_scope_narrowed_risk_cap_v1`，并将 "
        "`risk_appetite_refined_confidence_v1` current form archive。",
        "",
        f"- status: `{summary.get('status')}`",
        f"- market_regime: `{summary.get('market_regime')}`",
        f"- requested_date_range: `{summary.get('requested_date_range')}`",
        f"- active_record_count_total: `{summary.get('active_record_count_total')}`",
        "- eligible_active_record_count_total: "
        f"`{summary.get('eligible_active_record_count_total')}`",
        f"- source_data_quality_status: `{summary.get('source_data_quality_status')}`",
        "- risk_appetite_refined_confidence_v1: `current_form_archived`，不参与验证",
        "- baseline_plus_trend_structure_scope_narrowed_confirmation_v1: "
        "`confirmation_only` validation",
        "- volatility_regime_scope_narrowed_risk_cap_v1: `risk_cap_only` validation",
        f"- next_task_recommendation: `{summary.get('next_task_recommendation')}`",
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "## Active vs Inactive",
        "",
        "|candidate|usage|delta|label|",
        "|---|---|---:|---|",
    ]
    for row in comparison_rows:
        lines.append(
            "|`{}`|`{}`|{}|`{}`|".format(
                row.get("scope_narrowed_candidate_id"),
                row.get("usage_role"),
                row.get("active_vs_inactive_score_delta"),
                row.get("comparison_label"),
            )
        )
    lines.extend(
        [
            "",
            "## State Recommendation",
            "",
            "|candidate|status|forward_observe|",
            "|---|---|---:|",
        ]
    )
    for row in state_rows:
        lines.append(
            "|`{}`|`{}`|{}|".format(
                row.get("scope_narrowed_candidate_id"),
                row.get("recommended_research_status"),
                row.get("forward_observe_candidate_recommendation"),
            )
        )
    lines.extend(
        [
            "",
            "Forward observe candidate 仅为下一步 readiness review recommendation，"
            "不是 paper-shadow、production 或 broker readiness。",
            "本报告不生成新的 candidate、不修改 TRADING-2291 artifacts、不改变 "
            "TRADING-2281 / 2285 / 2289 / 2291 既有结论。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_confirmation_doc(scorecards: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Confirmation-Only Actual-Path Validation",
        "",
        "最后更新：2026-06-30",
        "",
        "|candidate|eligible|alignment|delta|false_confirmation|false_warning|status|",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in scorecards:
        lines.append(
            "|`{}`|{}|{}|{}|{}|{}|`{}`|".format(
                row.get("scope_narrowed_candidate_id"),
                row.get("active_eligible_count"),
                row.get("confirmation_alignment_rate"),
                row.get("active_vs_inactive_alignment_delta"),
                row.get("false_confirmation_count"),
                row.get("false_warning_count"),
                row.get("recommended_confirmation_status"),
            )
        )
    lines.extend(
        [
            "",
            "`confirmation_only` candidate 不是 primary directional signal；"
            "本报告只验证 active confirmation records 是否比 inactive reference "
            "更能确认趋势方向。",
            "当前 active scope worse than inactive reference，false confirmation / "
            "false warning cost 不支持继续按当前 confirmation scope 推进。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_risk_cap_doc(scorecards: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Risk-Cap-Only Actual-Path Validation",
        "",
        "最后更新：2026-06-30",
        "",
        "|candidate|eligible|capture|stress|tail|false_risk_cap|status|",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in scorecards:
        lines.append(
            "|`{}`|{}|{}|{}|{}|{}|`{}`|".format(
                row.get("scope_narrowed_candidate_id"),
                row.get("active_eligible_count"),
                row.get("risk_cap_capture_rate"),
                row.get("stress_event_capture_rate"),
                row.get("downside_tail_capture_rate"),
                row.get("false_risk_cap_count"),
                row.get("recommended_risk_cap_status"),
            )
        )
    lines.extend(
        [
            "",
            "`risk_cap_only` candidate 不要求市场最终一定下跌；若 active window "
            "出现 intrahorizon drawdown、stress 或 volatility expansion，也可构成 "
            "risk-cap alignment。",
            "该 family 的 2291 active records 只有 373 条，虽整体样本通过本轮 "
            "minimum active eligible floor，但 subgroup 解释仍应保持保守；"
            "forward observe candidate 只进入 TRADING-2293 readiness review，"
            "不得直接进入 paper-shadow。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_comparison_doc(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Scope-Narrowed Active vs Inactive Comparison",
        "",
        "最后更新：2026-06-30",
        "",
        "|candidate|active|inactive|active_score|inactive_score|delta|label|",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "|`{}`|{}|{}|{}|{}|{}|`{}`|".format(
                row.get("scope_narrowed_candidate_id"),
                row.get("active_eligible_count"),
                row.get("inactive_eligible_count"),
                row.get("active_alignment_score"),
                row.get("inactive_reference_score"),
                row.get("active_vs_inactive_score_delta"),
                row.get("comparison_label"),
            )
        )
    lines.extend(
        [
            "",
            "Inactive reference 不得用于 promotion evidence；它只帮助判断 active scope "
            "是否比被过滤 records 有更好边际证据。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_state_doc(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Scope-Narrowed State Recommendation",
        "",
        "最后更新：2026-06-30",
        "",
        f"- next_task_recommendation: `{summary.get('next_task_recommendation')}`",
        "- owner_review_candidate_count: `0`",
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "|candidate|status|forward_observe_candidate|sample|data_quality|",
        "|---|---|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "|`{}`|`{}`|{}|`{}`|`{}`|".format(
                row.get("scope_narrowed_candidate_id"),
                row.get("recommended_research_status"),
                row.get("forward_observe_candidate_recommendation"),
                row.get("sample_sufficiency_status"),
                row.get("data_quality_status"),
            )
        )
    lines.extend(
        [
            "",
            "允许状态只限 scope-narrowed research recommendation。禁止输出 "
            "`PROMOTION_READY`、`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 "
            "`BROKER_READY`。",
            "",
        ]
    )
    return "\n".join(lines)
