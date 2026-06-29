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

import pandas as pd

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
    write_json,
    write_markdown,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    REFINED_ARTIFACT_ROLE,
    REFINED_CANDIDATE_IDS,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    HIGH_CONFIDENCE_NEUTRAL_THRESHOLD,
    calculate_actual_path,
    classify_outcome_alignment,
    load_actual_price_matrix,
)
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list

DEFAULT_REFINED_GENERATOR_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "refined_candidate_generators_regenerated"
)
DEFAULT_ORIGINAL_VALIDATION_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "regenerated_candidate_actual_path_validation"
)
DEFAULT_REFINEMENT_PLAN_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "candidate_generator_confidence_scaling_refinement_plan"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "refined_candidate_actual_path_validation"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2289_REFINED_CANDIDATE_ACTUAL_PATH_VALIDATION"
STATUS = "REFINED_CANDIDATE_ACTUAL_PATH_EVIDENCE_READY_PROMOTION_BLOCKED"
MODE = "refined_actual_path_validation"
ARTIFACT_ROLE = "refined_candidate_actual_path_validation"
DEFAULT_CANDIDATES = tuple(REFINED_CANDIDATE_IDS.values())
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")

REQUIRED_REFINED_TOP_LEVEL_FILES = {
    "run_summary": "refined_regeneration_run_summary.json",
    "validation_summary": "refined_regeneration_validation_summary.json",
    "delta_summary": "refined_original_vs_refined_delta_summary.json",
}
REQUIRED_REFINED_CANDIDATE_FILES = {
    "signal_spec": "refined_candidate_signal_spec.json",
    "signal_series": "refined_candidate_signal_series.csv",
    "prediction_artifact": "refined_candidate_prediction_artifact.json",
    "generation_summary": "refined_generation_summary.json",
    "validation_summary": "refined_validation_summary.json",
    "parameter_application_report": "refined_parameter_application_report.json",
    "original_vs_refined_delta": "refined_original_vs_refined_delta.json",
}
REQUIRED_ORIGINAL_VALIDATION_FILES = {
    "summary": "regenerated_candidate_actual_path_validation_summary.json",
    "actual_path_matrix": "regenerated_candidate_actual_path_matrix.json",
    "outcome_matrix": "candidate_prediction_outcome_matrix.json",
    "scorecard": "candidate_validation_scorecard.json",
    "data_quality": "candidate_data_quality_report.json",
    "state_recommendation": "candidate_state_recommendation_matrix.json",
}
GUARDRAIL_MATRIX_FILENAME = "candidate_guardrail_matrix.json"

BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}
ALLOWED_REFINED_RESEARCH_STATUSES = {
    "REFINED_ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE",
    "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
    "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED",
    "REFINED_ACTUAL_PATH_VALIDATED_INCONCLUSIVE",
    "REFINED_DATA_QUALITY_BLOCKED",
    "REFINED_GUARDRAIL_BLOCKED",
}

# Research-only evidence classification baselines for TRADING-2289. Promotion,
# paper-shadow, production and broker gates remain explicitly disabled.
MINIMUM_HIGH_CONVICTION_RECORDS = 100
MINIMUM_ELIGIBLE_RECORDS_FOR_REFINED_REVIEW = 30
COMPARISON_MATERIALITY_THRESHOLD = 0.01


class RefinedCandidateActualPathValidationError(ValueError):
    pass


@dataclass(frozen=True)
class RefinedCandidateArtifacts:
    refined_candidate_id: str
    original_candidate_id: str
    candidate_dir: Path
    signal_spec: dict[str, Any]
    signal_series: list[dict[str, Any]]
    prediction_artifact: dict[str, Any]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    parameter_application_report: dict[str, Any]
    original_vs_refined_delta: dict[str, Any]
    artifact_paths: dict[str, Path]
    input_artifact_validation_status: str

    @property
    def prediction_records(self) -> list[dict[str, Any]]:
        records = self.prediction_artifact.get("prediction_records")
        if not isinstance(records, list):
            return []
        return [
            _refined_prediction_record(dict(row))
            for row in records
            if isinstance(row, Mapping)
        ]


@dataclass(frozen=True)
class RefinedCandidateActualPathInputs:
    refined_generator_dir: Path
    top_level_payloads: dict[str, dict[str, Any]]
    refined_artifacts: dict[str, RefinedCandidateArtifacts]
    original_validation: dict[str, Any]
    guardrail_payload: dict[str, Any]
    guardrail_rows: list[dict[str, Any]]


def run_refined_candidate_actual_path_validation(
    *,
    refined_generator_dir: Path = DEFAULT_REFINED_GENERATOR_ROOT,
    original_validation_dir: Path = DEFAULT_ORIGINAL_VALIDATION_ROOT,
    refinement_plan_dir: Path = DEFAULT_REFINEMENT_PLAN_ROOT,
    candidates: Sequence[str] | str = DEFAULT_CANDIDATES,
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
        raise RefinedCandidateActualPathValidationError(
            "refined candidate actual-path validation only supports "
            "refined_actual_path_validation mode"
        )
    refined_candidate_ids = _normalize_list(candidates)
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
    inputs = load_refined_candidate_actual_path_inputs(
        refined_generator_dir=refined_generator_dir,
        original_validation_dir=original_validation_dir,
        refinement_plan_dir=refinement_plan_dir,
        candidates=refined_candidate_ids,
    )

    actual_path_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []
    for bundle in inputs.refined_artifacts.values():
        for record in bundle.prediction_records:
            if str(record.get("target_asset")) not in asset_ids:
                continue
            if str(record.get("horizon")) not in horizon_ids:
                continue
            actual_path = calculate_actual_path(record, price_matrix)
            alignment = classify_outcome_alignment(record, actual_path)
            actual_row = refined_actual_path_row(record, actual_path)
            outcome_row = refined_prediction_outcome_row(record, actual_path, alignment)
            actual_path_rows.append(actual_row)
            outcome_rows.append(outcome_row)
            error_rows.append(refined_error_attribution_row(outcome_row))

    data_quality_rows = [
        build_refined_candidate_data_quality_row(
            refined_candidate_id=refined_candidate_id,
            original_candidate_id=inputs.refined_artifacts[
                refined_candidate_id
            ].original_candidate_id,
            actual_path_rows=_candidate_rows(actual_path_rows, refined_candidate_id),
            input_artifact_validation_status=inputs.refined_artifacts[
                refined_candidate_id
            ].input_artifact_validation_status,
        )
        for refined_candidate_id in refined_candidate_ids
    ]
    high_conviction_rows = build_refined_high_conviction_outcome_drilldown(outcome_rows)
    false_cost_rows = build_refined_false_signal_cost_matrix(outcome_rows)
    preliminary_scorecards = [
        build_refined_candidate_scorecard(
            refined_candidate_id,
            inputs.refined_artifacts[refined_candidate_id].original_candidate_id,
            _candidate_rows(outcome_rows, refined_candidate_id),
        )
        for refined_candidate_id in refined_candidate_ids
    ]
    comparison_base = build_original_vs_refined_actual_path_comparison(
        original_scorecards=inputs.original_validation["scorecards"],
        original_outcome_rows=inputs.original_validation["outcome_rows"],
        refined_scorecards=preliminary_scorecards,
        refined_outcome_rows=outcome_rows,
        refined_delta_rows=[
            bundle.original_vs_refined_delta for bundle in inputs.refined_artifacts.values()
        ],
        refined_data_quality_rows=data_quality_rows,
        guardrail_status_by_refined={},
    )
    guardrail_rows = build_refined_guardrail_validation_matrix(
        comparison_rows=comparison_base,
        refined_scorecards=preliminary_scorecards,
        guardrail_rows=inputs.guardrail_rows,
        refined_data_quality_rows=data_quality_rows,
    )
    guardrail_status_by_refined = {
        str(row.get("refined_candidate_id")): str(row.get("guardrail_status"))
        for row in guardrail_rows
    }
    comparison_rows = build_original_vs_refined_actual_path_comparison(
        original_scorecards=inputs.original_validation["scorecards"],
        original_outcome_rows=inputs.original_validation["outcome_rows"],
        refined_scorecards=preliminary_scorecards,
        refined_outcome_rows=outcome_rows,
        refined_delta_rows=[
            bundle.original_vs_refined_delta for bundle in inputs.refined_artifacts.values()
        ],
        refined_data_quality_rows=data_quality_rows,
        guardrail_status_by_refined=guardrail_status_by_refined,
    )
    state_rows = build_refined_candidate_state_recommendation_matrix(
        comparison_rows=comparison_rows,
        guardrail_rows=guardrail_rows,
        high_conviction_rows=high_conviction_rows,
        refined_scorecards=preliminary_scorecards,
        refined_data_quality_rows=data_quality_rows,
    )
    state_by_refined = {
        str(row.get("refined_candidate_id")): str(row.get("recommended_research_status"))
        for row in state_rows
    }
    scorecards = [
        {
            **row,
            "guardrail_status": guardrail_status_by_refined.get(
                str(row.get("refined_candidate_id")),
                "FAIL",
            ),
            "recommended_research_status": state_by_refined.get(
                str(row.get("refined_candidate_id")),
                "REFINED_ACTUAL_PATH_VALIDATED_INCONCLUSIVE",
            ),
            "owner_review_candidate_recommendation": _state_owner_review_candidate(
                state_by_refined.get(str(row.get("refined_candidate_id")), "")
            ),
        }
        for row in preliminary_scorecards
    ]
    summary = _summary(
        refined_candidate_ids=refined_candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        generated_at=generated_at,
        actual_path_rows=actual_path_rows,
        outcome_rows=outcome_rows,
        scorecards=scorecards,
        data_quality_rows=data_quality_rows,
        guardrail_rows=guardrail_rows,
        comparison_rows=comparison_rows,
        state_rows=state_rows,
        source_data_quality=data_quality,
        refined_generator_dir=refined_generator_dir,
        original_validation_dir=original_validation_dir,
        refinement_plan_dir=refinement_plan_dir,
        prices_path=prices_path,
    )
    common = _common_payload(generated_at=generated_at, summary=summary, mode=mode)
    paths = _artifact_paths(output_dir=output_dir, docs_root=docs_root)

    write_json(paths["summary"], {**common, "summary": summary})
    write_json(paths["actual_path_matrix_json"], {**common, "rows": actual_path_rows})
    _write_csv(paths["actual_path_matrix_csv"], actual_path_rows)
    write_json(paths["prediction_outcome_matrix_json"], {**common, "rows": outcome_rows})
    _write_csv(paths["prediction_outcome_matrix_csv"], outcome_rows)
    write_json(paths["scorecard"], {**common, "candidate_scorecards": scorecards})
    write_json(paths["high_conviction_json"], {**common, "rows": high_conviction_rows})
    _write_csv(paths["high_conviction_csv"], high_conviction_rows)
    write_json(paths["false_signal_cost_json"], {**common, "rows": false_cost_rows})
    _write_csv(paths["false_signal_cost_csv"], false_cost_rows)
    write_json(paths["guardrail_json"], {**common, "rows": guardrail_rows})
    _write_csv(paths["guardrail_csv"], guardrail_rows)
    write_json(paths["comparison_json"], {**common, "rows": comparison_rows})
    _write_csv(paths["comparison_csv"], comparison_rows)
    write_json(paths["state_recommendation_matrix"], {**common, "candidate_rows": state_rows})
    write_json(paths["error_attribution_seed"], {**common, "error_rows": error_rows})
    write_json(paths["data_quality_report"], {**common, "candidate_rows": data_quality_rows})
    write_markdown(paths["validation_report_doc"], _render_validation_report(summary, scorecards))
    write_markdown(paths["high_conviction_doc"], _render_high_conviction_doc(high_conviction_rows))
    write_markdown(paths["comparison_doc"], _render_comparison_doc(comparison_rows))
    write_markdown(paths["state_doc"], _render_state_doc(state_rows))

    payload = clean_for_yaml(
        {
            **common,
            "summary": summary,
            "candidate_scorecards": scorecards,
            "high_conviction_rows": high_conviction_rows,
            "guardrail_validation_rows": guardrail_rows,
            "comparison_rows": comparison_rows,
            "candidate_state_recommendations": state_rows,
            "candidate_data_quality": data_quality_rows,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "input_artifact_count": len(inputs.refined_artifacts),
            "actual_path_record_count": len(actual_path_rows),
            "prediction_outcome_record_count": len(outcome_rows),
            "refined_actual_path_validation_cli": "implemented",
            "refined_actual_path_matrix_generated": True,
            "refined_prediction_outcome_matrix_generated": True,
            "refined_candidate_scorecard_generated": True,
            "refined_high_conviction_outcome_drilldown_generated": True,
            "refined_guardrail_validation_matrix_generated": True,
            "original_vs_refined_actual_path_comparison_generated": True,
            "refined_candidate_state_recommendation_matrix_generated": True,
            "trading_2281_permanently_inconclusive_decisions_changed": False,
            "trading_2285_original_inconclusive_decisions_changed": False,
        }
    )
    _assert_generated_payload_safe("refined_candidate_actual_path_validation_result", payload)
    return payload


def load_refined_candidate_actual_path_inputs(
    *,
    refined_generator_dir: Path,
    original_validation_dir: Path,
    refinement_plan_dir: Path,
    candidates: Sequence[str] | str,
) -> RefinedCandidateActualPathInputs:
    refined_candidate_ids = _normalize_list(candidates)
    top_level_payloads = load_refined_top_level_artifacts(refined_generator_dir)
    refined_artifacts = load_refined_candidate_artifacts(
        refined_generator_dir,
        refined_candidate_ids,
    )
    original_validation = load_original_validation_outputs(original_validation_dir)
    guardrail_payload = load_refinement_guardrail_payload(refinement_plan_dir)
    guardrail_rows = _rows_from_payload(guardrail_payload)
    if not guardrail_rows:
        raise RefinedCandidateActualPathValidationError(
            f"{refinement_plan_dir / GUARDRAIL_MATRIX_FILENAME}: guardrail rows are empty"
        )
    return RefinedCandidateActualPathInputs(
        refined_generator_dir=refined_generator_dir,
        top_level_payloads=top_level_payloads,
        refined_artifacts=refined_artifacts,
        original_validation=original_validation,
        guardrail_payload=guardrail_payload,
        guardrail_rows=guardrail_rows,
    )


def load_refined_top_level_artifacts(refined_generator_dir: Path) -> dict[str, dict[str, Any]]:
    paths = {
        key: refined_generator_dir / filename
        for key, filename in REQUIRED_REFINED_TOP_LEVEL_FILES.items()
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise RefinedCandidateActualPathValidationError(
            f"missing refined top-level artifact(s): {missing}"
        )
    payloads = {key: _read_json(path) for key, path in paths.items()}
    for key, payload in payloads.items():
        _assert_input_payload_safe(f"refined_top_level.{key}", payload)
    validation = payloads["validation_summary"]
    if str(validation.get("status")) != "PASS":
        raise RefinedCandidateActualPathValidationError(
            "refined regeneration validation summary status must be PASS"
        )
    return payloads


def load_refined_candidate_artifacts(
    refined_generator_dir: Path,
    candidates: Sequence[str] | str,
) -> dict[str, RefinedCandidateArtifacts]:
    refined_candidate_ids = _normalize_list(candidates)
    loaded: dict[str, RefinedCandidateArtifacts] = {}
    for refined_candidate_id in refined_candidate_ids:
        candidate_dir = refined_generator_dir / refined_candidate_id
        paths = {
            key: candidate_dir / filename
            for key, filename in REQUIRED_REFINED_CANDIDATE_FILES.items()
        }
        missing = [str(path) for path in paths.values() if not path.exists()]
        if missing:
            raise RefinedCandidateActualPathValidationError(
                f"{refined_candidate_id}: missing refined artifact(s): {missing}"
            )
        signal_spec = _read_json(paths["signal_spec"])
        signal_series = _read_signal_series_csv(paths["signal_series"])
        prediction_artifact = _read_json(paths["prediction_artifact"])
        generation_summary = _read_json(paths["generation_summary"])
        validation_summary = _read_json(paths["validation_summary"])
        parameter_application_report = _read_json(paths["parameter_application_report"])
        original_vs_refined_delta = _read_json(paths["original_vs_refined_delta"])
        original_candidate_id = str(
            prediction_artifact.get("original_candidate_id")
            or signal_spec.get("original_candidate_id")
            or parameter_application_report.get("candidate_id")
        )
        input_status = validate_refined_candidate_artifact_bundle(
            refined_candidate_id=refined_candidate_id,
            original_candidate_id=original_candidate_id,
            signal_spec=signal_spec,
            signal_series=signal_series,
            prediction_artifact=prediction_artifact,
            validation_summary=validation_summary,
            parameter_application_report=parameter_application_report,
        )
        loaded[refined_candidate_id] = RefinedCandidateArtifacts(
            refined_candidate_id=refined_candidate_id,
            original_candidate_id=original_candidate_id,
            candidate_dir=candidate_dir,
            signal_spec=signal_spec,
            signal_series=signal_series,
            prediction_artifact=prediction_artifact,
            generation_summary=generation_summary,
            validation_summary=validation_summary,
            parameter_application_report=parameter_application_report,
            original_vs_refined_delta=original_vs_refined_delta,
            artifact_paths=paths,
            input_artifact_validation_status=input_status,
        )
    return loaded


def validate_refined_candidate_artifact_bundle(
    *,
    refined_candidate_id: str,
    original_candidate_id: str,
    signal_spec: Mapping[str, Any],
    signal_series: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    parameter_application_report: Mapping[str, Any],
) -> str:
    validator = CandidateSignalBindingValidator()
    results = [
        validator.validate_candidate_signal_spec(signal_spec),
        validator.validate_candidate_bound_signal_series(signal_series),
        validator.validate_candidate_bound_prediction_artifact(prediction_artifact),
    ]
    errors = [error for result in results for error in result.errors]
    if str(signal_spec.get("candidate_id")) != refined_candidate_id:
        errors.append(f"{refined_candidate_id}: signal spec candidate_id mismatch")
    if str(prediction_artifact.get("candidate_id")) != refined_candidate_id:
        errors.append(f"{refined_candidate_id}: prediction artifact candidate_id mismatch")
    if str(prediction_artifact.get("refined_candidate_id")) != refined_candidate_id:
        errors.append(f"{refined_candidate_id}: prediction artifact refined_candidate_id mismatch")
    if not original_candidate_id:
        errors.append(f"{refined_candidate_id}: missing original_candidate_id")
    if original_candidate_id == refined_candidate_id:
        errors.append(f"{refined_candidate_id}: refined_candidate_id equals original_candidate_id")
    if prediction_artifact.get("artifact_role") != REFINED_ARTIFACT_ROLE:
        errors.append(f"{refined_candidate_id}: prediction artifact role is not refined")
    if str(validation_summary.get("status")) != "PASS":
        errors.append(f"{refined_candidate_id}: refined_validation_summary status is not PASS")
    selected_sets = _strings(
        prediction_artifact.get("selected_parameter_set_ids")
        or parameter_application_report.get("selected_parameter_set_ids")
    )
    if not selected_sets:
        errors.append(f"{refined_candidate_id}: missing selected_parameter_set_ids")
    errors.extend(_refined_payload_contract_errors(refined_candidate_id, prediction_artifact))
    for index, record in enumerate(prediction_artifact.get("prediction_records", [])):
        if not isinstance(record, Mapping):
            errors.append(
                f"{refined_candidate_id}.prediction_records[{index}]: record is not object"
            )
            continue
        errors.extend(
            _refined_payload_contract_errors(
                f"{refined_candidate_id}.prediction_records[{index}]",
                record,
            )
        )
    if errors:
        raise RefinedCandidateActualPathValidationError("; ".join(errors))
    return "PASS"


def load_original_validation_outputs(original_validation_dir: Path) -> dict[str, Any]:
    paths = {
        key: original_validation_dir / filename
        for key, filename in REQUIRED_ORIGINAL_VALIDATION_FILES.items()
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise RefinedCandidateActualPathValidationError(
            f"missing original validation artifact(s): {missing}"
        )
    payloads = {key: _read_json(path) for key, path in paths.items()}
    for key, payload in payloads.items():
        _assert_input_payload_safe(f"original_validation.{key}", payload)
        _assert_no_banned_recommendations(f"original_validation.{key}", payload)
    scorecards = _scorecards_from_payload(payloads["scorecard"])
    outcome_rows = _rows_from_payload(payloads["outcome_matrix"])
    if not scorecards:
        raise RefinedCandidateActualPathValidationError(
            "original validation scorecard is missing candidate_scorecards"
        )
    if not outcome_rows:
        raise RefinedCandidateActualPathValidationError(
            "original validation outcome matrix is missing rows"
        )
    return {
        **payloads,
        "scorecards": scorecards,
        "outcome_rows": outcome_rows,
        "data_quality_rows": _candidate_rows_from_payload(payloads["data_quality"]),
        "state_rows": _candidate_rows_from_payload(payloads["state_recommendation"]),
    }


def load_refinement_guardrail_payload(refinement_plan_dir: Path) -> dict[str, Any]:
    path = refinement_plan_dir / GUARDRAIL_MATRIX_FILENAME
    if not path.exists():
        raise RefinedCandidateActualPathValidationError(f"missing guardrail matrix: {path}")
    payload = _read_json(path)
    _assert_input_payload_safe("refinement_plan.guardrails", payload)
    _assert_no_banned_recommendations("refinement_plan.guardrails", payload)
    return payload


def refined_actual_path_row(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "refined_candidate_id": record.get("refined_candidate_id")
            or record.get("candidate_id"),
            "original_candidate_id": record.get("original_candidate_id"),
            "candidate_id": record.get("candidate_id"),
            "candidate_family": record.get("candidate_family"),
            "target_asset": record.get("target_asset"),
            "horizon": record.get("horizon"),
            "decision_timestamp": record.get("decision_timestamp"),
            "valid_from": record.get("valid_from"),
            "valid_until": record.get("valid_until"),
            "signal_name": record.get("signal_name"),
            "signal_direction": record.get("signal_direction"),
            "signal_value": to_float(record.get("signal_value")),
            "signal_confidence": to_float(record.get("signal_confidence")),
            "refined_signal_value": to_float(record.get("refined_signal_value")),
            "refined_signal_confidence": to_float(record.get("refined_signal_confidence")),
            "high_conviction_flag": _bool(record.get("high_conviction_flag")),
            **dict(actual_path),
            **_safety_fields(),
        }
    )


def refined_prediction_outcome_row(
    record: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    alignment: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "record_id": _record_id(record),
            "refined_candidate_id": record.get("refined_candidate_id")
            or record.get("candidate_id"),
            "original_candidate_id": record.get("original_candidate_id"),
            "candidate_id": record.get("candidate_id"),
            "target_asset": record.get("target_asset"),
            "decision_timestamp": record.get("decision_timestamp"),
            "horizon": record.get("horizon"),
            "signal_name": record.get("signal_name"),
            "signal_direction": record.get("signal_direction"),
            "signal_value": to_float(record.get("signal_value")),
            "signal_confidence": to_float(record.get("signal_confidence")),
            "refined_signal_value": to_float(record.get("refined_signal_value")),
            "refined_signal_confidence": to_float(record.get("refined_signal_confidence")),
            "high_conviction_flag": _bool(record.get("high_conviction_flag")),
            "actual_path_status": actual_path.get("actual_path_status"),
            "validation_eligible": actual_path.get("validation_eligible"),
            "data_quality_warning": actual_path.get("data_quality_warning"),
            "actual_forward_return": actual_path.get("forward_return"),
            "actual_max_drawdown": actual_path.get("max_drawdown_during_horizon"),
            "actual_max_runup": actual_path.get("max_runup_during_horizon"),
            "actual_realized_volatility": actual_path.get("realized_volatility"),
            "alignment_label": alignment.get("alignment_label"),
            "alignment_score": alignment.get("alignment_score"),
            "error_type": alignment.get("error_type"),
            "dominant_observed_driver": _dominant_observed_driver(actual_path),
            **_safety_fields(),
        }
    )


def build_refined_candidate_scorecard(
    refined_candidate_id: str,
    original_candidate_id: str,
    outcome_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    record_count = len(outcome_rows)
    eligible = _eligible_rows(outcome_rows)
    high = [row for row in eligible if _bool(row.get("high_conviction_flag"))]
    warning_count = sum(1 for row in outcome_rows if bool(row.get("data_quality_warning")))
    false_risk_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
    false_risk_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
    scores = [to_float(row.get("alignment_score")) for row in eligible]
    confidence_weights = [
        max(
            to_float(row.get("refined_signal_confidence") or row.get("signal_confidence")),
            0.0,
        )
        for row in eligible
    ]
    high_scores = [to_float(row.get("alignment_score")) for row in high]
    high_confidence_weights = [
        max(
            to_float(row.get("refined_signal_confidence") or row.get("signal_confidence")),
            0.0,
        )
        for row in high
    ]
    alignment_rate = _alignment_rate(eligible)
    weighted_alignment_score = _mean(scores)
    confidence_weighted = _weighted_mean(scores, confidence_weights)
    high_alignment_rate = _alignment_rate(high)
    high_weighted_alignment = _mean(high_scores)
    high_confidence_weighted = _weighted_mean(high_scores, high_confidence_weights)
    horizon_scores = _average_by(eligible, "horizon", "alignment_score")
    asset_scores = _average_by(eligible, "target_asset", "alignment_score")
    false_risk_on_cost = _error_cost(false_risk_on)
    false_risk_off_cost = _error_cost(false_risk_off)
    return clean_for_yaml(
        {
            "refined_candidate_id": refined_candidate_id,
            "original_candidate_id": original_candidate_id,
            "candidate_id": refined_candidate_id,
            "record_count": record_count,
            "validation_eligible_record_count": len(eligible),
            "validation_ineligible_record_count": record_count - len(eligible),
            "data_quality_warning_count": warning_count,
            "alignment_rate": round_float(alignment_rate),
            "weighted_alignment_score": round_float(weighted_alignment_score),
            "confidence_weighted_alignment_score": round_float(confidence_weighted),
            "high_conviction_record_count": sum(
                1 for row in outcome_rows if _bool(row.get("high_conviction_flag"))
            ),
            "high_conviction_eligible_record_count": len(high),
            "high_conviction_alignment_rate": round_float(high_alignment_rate),
            "high_conviction_weighted_alignment_score": round_float(high_weighted_alignment),
            "high_conviction_confidence_weighted_alignment_score": round_float(
                high_confidence_weighted
            ),
            "false_risk_on_count": len(false_risk_on),
            "false_risk_off_count": len(false_risk_off),
            "false_risk_on_cost": round_float(false_risk_on_cost),
            "false_risk_off_cost": round_float(false_risk_off_cost),
            "average_forward_return_by_direction": _average_by(
                outcome_rows,
                "signal_direction",
                "actual_forward_return",
            ),
            "average_max_drawdown_by_direction": _average_by(
                outcome_rows,
                "signal_direction",
                "actual_max_drawdown",
            ),
            "average_realized_volatility_by_direction": _average_by(
                outcome_rows,
                "signal_direction",
                "actual_realized_volatility",
            ),
            "best_horizon": max(horizon_scores, key=horizon_scores.get) if horizon_scores else "",
            "worst_horizon": min(horizon_scores, key=horizon_scores.get) if horizon_scores else "",
            "best_asset": max(asset_scores, key=asset_scores.get) if asset_scores else "",
            "worst_asset": min(asset_scores, key=asset_scores.get) if asset_scores else "",
            "guardrail_status": "PENDING",
            "recommended_research_status": _preliminary_refined_research_status(
                record_count=record_count,
                eligible_count=len(eligible),
                confidence_weighted_score=confidence_weighted,
            ),
            "owner_review_candidate_recommendation": False,
            **_safety_fields(),
            **_recommendation_safety_fields(),
            **_boundary_fields(),
        }
    )


def build_refined_candidate_data_quality_row(
    *,
    refined_candidate_id: str,
    original_candidate_id: str,
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
            "refined_candidate_id": refined_candidate_id,
            "original_candidate_id": original_candidate_id,
            "candidate_id": refined_candidate_id,
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


def build_refined_high_conviction_outcome_drilldown(
    outcome_rows: Sequence[Mapping[str, Any]],
    *,
    minimum_high_conviction_records: int = MINIMUM_HIGH_CONVICTION_RECORDS,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in outcome_rows:
        key = (
            str(row.get("refined_candidate_id") or row.get("candidate_id")),
            str(row.get("original_candidate_id")),
            str(row.get("target_asset")),
            str(row.get("horizon")),
        )
        grouped[key].append(row)
    result: list[dict[str, Any]] = []
    for (refined_id, original_id, target_asset, horizon), rows in sorted(grouped.items()):
        high_rows = [row for row in rows if _bool(row.get("high_conviction_flag"))]
        non_high_rows = [row for row in rows if not _bool(row.get("high_conviction_flag"))]
        high_eligible = _eligible_rows(high_rows)
        non_high_eligible = _eligible_rows(non_high_rows)
        high_false_on = [row for row in high_eligible if row.get("error_type") == "false_risk_on"]
        high_false_off = [
            row for row in high_eligible if row.get("error_type") == "false_risk_off"
        ]
        non_high_false = [
            row
            for row in non_high_eligible
            if row.get("error_type") in {"false_risk_on", "false_risk_off"}
        ]
        high_false_cost = _error_cost(high_false_on) + _error_cost(high_false_off)
        non_high_false_cost = _error_cost(non_high_false)
        high_cost_rate = high_false_cost / len(high_eligible) if high_eligible else 0.0
        non_high_cost_rate = (
            non_high_false_cost / len(non_high_eligible) if non_high_eligible else 0.0
        )
        alignment_delta = _alignment_rate(high_eligible) - _alignment_rate(non_high_eligible)
        false_cost_delta = high_cost_rate - non_high_cost_rate
        label = _high_conviction_label(
            high_eligible_count=len(high_eligible),
            alignment_delta=alignment_delta,
            false_cost_delta=false_cost_delta,
            minimum_high_conviction_records=minimum_high_conviction_records,
        )
        result.append(
            clean_for_yaml(
                {
                    "refined_candidate_id": refined_id,
                    "original_candidate_id": original_id,
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "high_conviction_record_count": len(high_rows),
                    "high_conviction_eligible_count": len(high_eligible),
                    "high_conviction_alignment_rate": round_float(
                        _alignment_rate(high_eligible)
                    ),
                    "high_conviction_weighted_alignment_score": round_float(
                        _mean([to_float(row.get("alignment_score")) for row in high_eligible])
                    ),
                    "high_conviction_false_risk_on_count": len(high_false_on),
                    "high_conviction_false_risk_off_count": len(high_false_off),
                    "high_conviction_false_risk_on_cost": round_float(
                        _error_cost(high_false_on)
                    ),
                    "high_conviction_false_risk_off_cost": round_float(
                        _error_cost(high_false_off)
                    ),
                    "high_conviction_average_forward_return": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_forward_return"))
                                for row in high_eligible
                            ]
                        )
                    ),
                    "high_conviction_average_max_drawdown": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_max_drawdown"))
                                for row in high_eligible
                            ]
                        )
                    ),
                    "high_conviction_average_realized_volatility": round_float(
                        _mean(
                            [
                                to_float(row.get("actual_realized_volatility"))
                                for row in high_eligible
                            ]
                        )
                    ),
                    "non_high_conviction_alignment_rate": round_float(
                        _alignment_rate(non_high_eligible)
                    ),
                    "non_high_conviction_weighted_alignment_score": round_float(
                        _mean(
                            [
                                to_float(row.get("alignment_score"))
                                for row in non_high_eligible
                            ]
                        )
                    ),
                    "high_vs_non_high_alignment_delta": round_float(alignment_delta),
                    "high_vs_non_high_false_cost_delta": round_float(false_cost_delta),
                    "high_conviction_outcome_label": label,
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return result


def build_refined_false_signal_cost_matrix(
    outcome_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in outcome_rows:
        grouped[
            (
                str(row.get("refined_candidate_id") or row.get("candidate_id")),
                str(row.get("original_candidate_id")),
                str(row.get("target_asset")),
                str(row.get("horizon")),
            )
        ].append(row)
    result: list[dict[str, Any]] = []
    for (refined_id, original_id, target_asset, horizon), rows in sorted(grouped.items()):
        eligible = _eligible_rows(rows)
        false_on = [row for row in eligible if row.get("error_type") == "false_risk_on"]
        false_off = [row for row in eligible if row.get("error_type") == "false_risk_off"]
        result.append(
            clean_for_yaml(
                {
                    "refined_candidate_id": refined_id,
                    "original_candidate_id": original_id,
                    "target_asset": target_asset,
                    "horizon": horizon,
                    "eligible_record_count": len(eligible),
                    "false_risk_on_count": len(false_on),
                    "false_risk_off_count": len(false_off),
                    "false_risk_on_cost": round_float(_error_cost(false_on)),
                    "false_risk_off_cost": round_float(_error_cost(false_off)),
                    "total_false_signal_cost": round_float(
                        _error_cost(false_on) + _error_cost(false_off)
                    ),
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return result


def build_original_vs_refined_actual_path_comparison(
    *,
    original_scorecards: Sequence[Mapping[str, Any]],
    original_outcome_rows: Sequence[Mapping[str, Any]],
    refined_scorecards: Sequence[Mapping[str, Any]],
    refined_outcome_rows: Sequence[Mapping[str, Any]],
    refined_delta_rows: Sequence[Mapping[str, Any]] = (),
    refined_data_quality_rows: Sequence[Mapping[str, Any]] = (),
    guardrail_status_by_refined: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    original_score_by_candidate = {
        str(row.get("candidate_id")): dict(row) for row in original_scorecards
    }
    refined_outcomes_by_candidate = _rows_by_key(refined_outcome_rows, "refined_candidate_id")
    original_outcomes_by_candidate = _rows_by_key(original_outcome_rows, "candidate_id")
    delta_by_refined = {
        str(row.get("refined_candidate_id")): dict(row) for row in refined_delta_rows
    }
    data_quality_by_refined = {
        str(row.get("refined_candidate_id") or row.get("candidate_id")): str(
            row.get("data_quality_status") or "PASS"
        )
        for row in refined_data_quality_rows
    }
    guardrail_map = dict(guardrail_status_by_refined or {})
    rows: list[dict[str, Any]] = []
    for refined_score in refined_scorecards:
        refined_id = str(
            refined_score.get("refined_candidate_id") or refined_score.get("candidate_id")
        )
        original_id = str(refined_score.get("original_candidate_id"))
        original_score = original_score_by_candidate.get(original_id, {})
        original_rows = original_outcomes_by_candidate.get(original_id, [])
        refined_rows = refined_outcomes_by_candidate.get(refined_id, [])
        delta = delta_by_refined.get(refined_id, {})
        high_refined = [row for row in refined_rows if _bool(row.get("high_conviction_flag"))]
        original_alignment = to_float(original_score.get("alignment_rate"))
        refined_alignment = to_float(refined_score.get("alignment_rate"))
        original_weighted = to_float(original_score.get("weighted_alignment_score"))
        refined_weighted = to_float(refined_score.get("weighted_alignment_score"))
        original_conf_weighted = to_float(
            original_score.get("confidence_weighted_alignment_score")
        )
        refined_conf_weighted = to_float(
            refined_score.get("confidence_weighted_alignment_score")
        )
        false_on_delta = to_float(refined_score.get("false_risk_on_cost")) - to_float(
            original_score.get("false_risk_on_cost")
        )
        false_off_delta = to_float(refined_score.get("false_risk_off_cost")) - to_float(
            original_score.get("false_risk_off_cost")
        )
        data_quality_refined = data_quality_by_refined.get(refined_id, "PASS")
        guardrail_status = guardrail_map.get(refined_id, "PENDING")
        high_false_cost = _error_cost(
            [
                row
                for row in _eligible_rows(high_refined)
                if row.get("error_type") in {"false_risk_on", "false_risk_off"}
            ]
        )
        comparison_label = _comparison_label(
            confidence_weighted_score_delta=refined_conf_weighted - original_conf_weighted,
            alignment_rate_delta=refined_alignment - original_alignment,
            false_risk_on_cost_delta=false_on_delta,
            false_risk_off_cost_delta=false_off_delta,
            high_conviction_alignment_rate_refined=_alignment_rate(_eligible_rows(high_refined)),
            alignment_rate_original=original_alignment,
            guardrail_status=guardrail_status,
            data_quality_status_refined=data_quality_refined,
        )
        rows.append(
            clean_for_yaml(
                {
                    "original_candidate_id": original_id,
                    "refined_candidate_id": refined_id,
                    "record_count_original": int(
                        original_score.get("record_count") or len(original_rows)
                    ),
                    "record_count_refined": int(
                        refined_score.get("record_count") or len(refined_rows)
                    ),
                    "eligible_count_original": int(
                        original_score.get("validation_eligible_record_count") or 0
                    ),
                    "eligible_count_refined": int(
                        refined_score.get("validation_eligible_record_count") or 0
                    ),
                    "neutral_ratio_original": round_float(
                        delta.get("neutral_ratio_original")
                        if "neutral_ratio_original" in delta
                        else _neutral_ratio(original_rows)
                    ),
                    "neutral_ratio_refined": round_float(
                        delta.get("neutral_ratio_refined")
                        if "neutral_ratio_refined" in delta
                        else _neutral_ratio(refined_rows)
                    ),
                    "directional_signal_ratio_original": round_float(
                        delta.get("directional_signal_ratio_original")
                        if "directional_signal_ratio_original" in delta
                        else 1.0 - _neutral_ratio(original_rows)
                    ),
                    "directional_signal_ratio_refined": round_float(
                        delta.get("directional_signal_ratio_refined")
                        if "directional_signal_ratio_refined" in delta
                        else 1.0 - _neutral_ratio(refined_rows)
                    ),
                    "high_confidence_ratio_original": round_float(
                        delta.get("high_confidence_ratio_original")
                        if "high_confidence_ratio_original" in delta
                        else _high_confidence_ratio(original_rows)
                    ),
                    "high_confidence_ratio_refined": round_float(
                        delta.get("high_confidence_ratio_refined")
                        if "high_confidence_ratio_refined" in delta
                        else _high_confidence_ratio(refined_rows)
                    ),
                    "alignment_rate_original": round_float(original_alignment),
                    "alignment_rate_refined": round_float(refined_alignment),
                    "alignment_rate_delta": round_float(refined_alignment - original_alignment),
                    "weighted_alignment_score_original": round_float(original_weighted),
                    "weighted_alignment_score_refined": round_float(refined_weighted),
                    "weighted_alignment_score_delta": round_float(
                        refined_weighted - original_weighted
                    ),
                    "confidence_weighted_score_original": round_float(original_conf_weighted),
                    "confidence_weighted_score_refined": round_float(refined_conf_weighted),
                    "confidence_weighted_score_delta": round_float(
                        refined_conf_weighted - original_conf_weighted
                    ),
                    "false_risk_on_cost_original": round_float(
                        to_float(original_score.get("false_risk_on_cost"))
                    ),
                    "false_risk_on_cost_refined": round_float(
                        to_float(refined_score.get("false_risk_on_cost"))
                    ),
                    "false_risk_on_cost_delta": round_float(false_on_delta),
                    "false_risk_off_cost_original": round_float(
                        to_float(original_score.get("false_risk_off_cost"))
                    ),
                    "false_risk_off_cost_refined": round_float(
                        to_float(refined_score.get("false_risk_off_cost"))
                    ),
                    "false_risk_off_cost_delta": round_float(false_off_delta),
                    "high_conviction_alignment_rate_refined": round_float(
                        _alignment_rate(_eligible_rows(high_refined))
                    ),
                    "high_conviction_false_cost_refined": round_float(high_false_cost),
                    "data_quality_status_original": "PASS",
                    "data_quality_status_refined": data_quality_refined,
                    "guardrail_status": guardrail_status,
                    "comparison_label": comparison_label,
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return rows


def build_refined_guardrail_validation_matrix(
    *,
    comparison_rows: Sequence[Mapping[str, Any]],
    refined_scorecards: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    refined_data_quality_rows: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    policy_by_original = _guardrail_policy_by_original_candidate(guardrail_rows)
    score_by_refined = {
        str(row.get("refined_candidate_id") or row.get("candidate_id")): row
        for row in refined_scorecards
    }
    data_quality_by_refined = {
        str(row.get("refined_candidate_id") or row.get("candidate_id")): str(
            row.get("data_quality_status") or "PASS"
        )
        for row in refined_data_quality_rows
    }
    rows: list[dict[str, Any]] = []
    for comparison in comparison_rows:
        original_id = str(comparison.get("original_candidate_id"))
        refined_id = str(comparison.get("refined_candidate_id"))
        policy = policy_by_original.get(original_id)
        score = score_by_refined.get(refined_id, {})
        data_quality_status = data_quality_by_refined.get(refined_id, "PASS")
        if not policy:
            rows.append(
                clean_for_yaml(
                    {
                        "refined_candidate_id": refined_id,
                        "original_candidate_id": original_id,
                        "guardrail_profile": "",
                        "guardrail_status": "FAIL",
                        "validation_status": "FAIL_CLOSED",
                        "guardrail_fail_reasons": ["missing_guardrail_profile"],
                        **_empty_guardrail_metrics(comparison, score, data_quality_status),
                        **_safety_fields(),
                        **_recommendation_safety_fields(),
                    }
                )
            )
            continue
        fail_reasons: list[str] = []
        actual_high = to_float(comparison.get("high_confidence_ratio_refined"))
        max_high = to_float(policy.get("max_high_confidence_ratio"), default=1.0)
        false_on_delta = to_float(comparison.get("false_risk_on_cost_delta"))
        false_off_delta = to_float(comparison.get("false_risk_off_cost_delta"))
        max_false_on = to_float(policy.get("max_false_risk_on_cost_increase"))
        max_false_off = to_float(policy.get("max_false_risk_off_cost_increase"))
        neutral_reduction = max(
            to_float(comparison.get("neutral_ratio_original"))
            - to_float(comparison.get("neutral_ratio_refined")),
            0.0,
        )
        max_neutral_reduction = to_float(policy.get("max_neutral_ratio_reduction"), default=1.0)
        eligible = int(score.get("validation_eligible_record_count") or 0)
        min_eligible = int(to_float(policy.get("minimum_eligible_records")))
        required_statuses = set(_strings(policy.get("data_quality_required_status"))) or {
            "PASS",
            "PASS_WITH_WARNINGS",
        }
        if actual_high > max_high:
            fail_reasons.append("max_high_confidence_ratio_exceeded")
        if false_on_delta > max_false_on:
            fail_reasons.append("false_risk_on_cost_increase_exceeded")
        if false_off_delta > max_false_off:
            fail_reasons.append("false_risk_off_cost_increase_exceeded")
        if neutral_reduction > max_neutral_reduction:
            fail_reasons.append("neutral_ratio_reduction_exceeded")
        if eligible < min_eligible:
            fail_reasons.append("minimum_eligible_records_not_met")
        if data_quality_status not in required_statuses:
            fail_reasons.append("data_quality_status_not_allowed")
        guardrail_status = (
            "FAIL"
            if fail_reasons
            else "PASS_WITH_WARNINGS"
            if data_quality_status == "PASS_WITH_WARNINGS"
            else "PASS"
        )
        rows.append(
            clean_for_yaml(
                {
                    "refined_candidate_id": refined_id,
                    "original_candidate_id": original_id,
                    "guardrail_profile": policy.get("guardrail_profile"),
                    "max_high_confidence_ratio": round_float(max_high),
                    "actual_high_confidence_ratio": round_float(actual_high),
                    "max_false_risk_on_cost_increase": round_float(max_false_on),
                    "actual_false_risk_on_cost_delta": round_float(false_on_delta),
                    "max_false_risk_off_cost_increase": round_float(max_false_off),
                    "actual_false_risk_off_cost_delta": round_float(false_off_delta),
                    "max_neutral_ratio_reduction": round_float(max_neutral_reduction),
                    "actual_neutral_ratio_reduction": round_float(neutral_reduction),
                    "minimum_eligible_records": min_eligible,
                    "actual_eligible_records": eligible,
                    "data_quality_required_status": sorted(required_statuses),
                    "actual_data_quality_status": data_quality_status,
                    "guardrail_status": guardrail_status,
                    "validation_status": "PASS" if guardrail_status != "FAIL" else "FAIL_CLOSED",
                    "guardrail_fail_reasons": fail_reasons,
                    **_safety_fields(),
                    **_recommendation_safety_fields(),
                }
            )
        )
    return rows


def build_refined_candidate_state_recommendation_matrix(
    *,
    comparison_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    high_conviction_rows: Sequence[Mapping[str, Any]],
    refined_scorecards: Sequence[Mapping[str, Any]],
    refined_data_quality_rows: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    guardrail_by_refined = {
        str(row.get("refined_candidate_id")): row for row in guardrail_rows
    }
    high_by_refined = _rows_by_key(high_conviction_rows, "refined_candidate_id")
    score_by_refined = {
        str(row.get("refined_candidate_id") or row.get("candidate_id")): row
        for row in refined_scorecards
    }
    data_quality_by_refined = {
        str(row.get("refined_candidate_id") or row.get("candidate_id")): str(
            row.get("data_quality_status") or "PASS"
        )
        for row in refined_data_quality_rows
    }
    rows: list[dict[str, Any]] = []
    for comparison in comparison_rows:
        refined_id = str(comparison.get("refined_candidate_id"))
        original_id = str(comparison.get("original_candidate_id"))
        guardrail = guardrail_by_refined.get(refined_id, {})
        score = score_by_refined.get(refined_id, {})
        dq_status = data_quality_by_refined.get(refined_id, "PASS")
        high_labels = {
            str(row.get("high_conviction_outcome_label"))
            for row in high_by_refined.get(refined_id, [])
        }
        status = _state_recommendation(
            comparison_label=str(comparison.get("comparison_label")),
            guardrail_status=str(guardrail.get("guardrail_status") or "FAIL"),
            data_quality_status=dq_status,
            eligible_count=int(score.get("validation_eligible_record_count") or 0),
            high_conviction_labels=high_labels,
        )
        if status not in ALLOWED_REFINED_RESEARCH_STATUSES:
            raise RefinedCandidateActualPathValidationError(
                f"{refined_id}: unsupported refined recommendation {status}"
            )
        rows.append(
            clean_for_yaml(
                {
                    "refined_candidate_id": refined_id,
                    "original_candidate_id": original_id,
                    "recommended_research_status": status,
                    "owner_review_candidate_recommendation": _state_owner_review_candidate(status),
                    "guardrail_status": guardrail.get("guardrail_status"),
                    "comparison_label": comparison.get("comparison_label"),
                    "data_quality_status": dq_status,
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                    **candidate_artifact_safety_fields(),
                    **_recommendation_safety_fields(),
                    **_boundary_fields(),
                }
            )
        )
    return rows


def refined_error_attribution_row(row: Mapping[str, Any]) -> dict[str, Any]:
    error_type = str(row.get("error_type") or "no_error")
    high_flag = _bool(row.get("high_conviction_flag"))
    if high_flag and error_type == "false_risk_on":
        refined_error = "high_conviction_false_positive"
    elif high_flag and error_type == "false_risk_off":
        refined_error = "high_conviction_false_positive"
    elif error_type in {
        "false_risk_on",
        "false_risk_off",
        "volatility_misclassification",
        "neutral_misclassification",
        "data_quality_inconclusive",
    }:
        refined_error = error_type
    else:
        refined_error = "no_error"
    return clean_for_yaml(
        {
            "refined_candidate_id": row.get("refined_candidate_id") or row.get("candidate_id"),
            "original_candidate_id": row.get("original_candidate_id"),
            "candidate_id": row.get("candidate_id"),
            "record_id": row.get("record_id"),
            "target_asset": row.get("target_asset"),
            "decision_timestamp": row.get("decision_timestamp"),
            "horizon": row.get("horizon"),
            "signal_direction": row.get("signal_direction"),
            "refined_signal_value": row.get("refined_signal_value"),
            "refined_signal_confidence": row.get("refined_signal_confidence"),
            "high_conviction_flag": high_flag,
            "actual_forward_return": row.get("actual_forward_return"),
            "actual_max_drawdown": row.get("actual_max_drawdown"),
            "actual_realized_volatility": row.get("actual_realized_volatility"),
            "alignment_label": row.get("alignment_label"),
            "error_type": refined_error,
            "dominant_observed_driver": row.get("dominant_observed_driver"),
            "owner_review_note": _owner_review_note(refined_error),
            **_safety_fields(),
            **_recommendation_safety_fields(),
        }
    )


def _summary(
    *,
    refined_candidate_ids: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    generated_at: datetime,
    actual_path_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    scorecards: Sequence[Mapping[str, Any]],
    data_quality_rows: Sequence[Mapping[str, Any]],
    guardrail_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    source_data_quality: Mapping[str, Any],
    refined_generator_dir: Path,
    original_validation_dir: Path,
    refinement_plan_dir: Path,
    prices_path: Path,
) -> dict[str, Any]:
    eligible_count = sum(1 for row in actual_path_rows if bool(row.get("validation_eligible")))
    status_counts = Counter(str(row.get("actual_path_status")) for row in actual_path_rows)
    guardrail_counts = Counter(str(row.get("guardrail_status")) for row in guardrail_rows)
    comparison_counts = Counter(str(row.get("comparison_label")) for row in comparison_rows)
    recommendation_counts = Counter(
        str(row.get("recommended_research_status")) for row in state_rows
    )
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
            "anchor_event": "ChatGPT public launch",
            "anchor_date": "2022-11-30",
            "default_backtest_start": "2022-12-01",
            "refined_generator_dir": str(refined_generator_dir),
            "original_validation_dir": str(original_validation_dir),
            "refinement_plan_dir": str(refinement_plan_dir),
            "prices_path": str(prices_path),
            "candidate_count": len(refined_candidate_ids),
            "candidate_ids": list(refined_candidate_ids),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "actual_path_record_count": len(actual_path_rows),
            "prediction_outcome_record_count": len(outcome_rows),
            "validation_eligible_record_count": eligible_count,
            "validation_ineligible_record_count": len(actual_path_rows) - eligible_count,
            "actual_path_status_counts": dict(status_counts),
            "guardrail_status_counts": dict(guardrail_counts),
            "comparison_label_counts": dict(comparison_counts),
            "candidate_recommendation_counts": dict(recommendation_counts),
            "data_quality_status": output_data_quality_status,
            "source_data_quality_status": source_data_quality.get("status"),
            "source_data_quality_error_count": source_data_quality.get("error_count"),
            "source_data_quality_warning_count": source_data_quality.get("warning_count"),
            "artifact_role": ARTIFACT_ROLE,
            "high_confidence_ratio_is_not_strategy_validity": True,
            "actual_path_validation_executed": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            "broker_action_recommendation_allowed": False,
            "next_task_options": [
                "TRADING-2290_Refined_Candidate_Owner_Review_Package",
                "TRADING-2290_Refined_Candidate_Local_Edge_And_Scope_Narrowing_Review",
                "TRADING-2290_Reject_Refined_Confidence_Scaling_And_Redesign_Candidate_Family",
            ],
            **_boundary_fields(),
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    summary: Mapping[str, Any],
    mode: str,
) -> dict[str, Any]:
    return {
        "schema_version": "refined_candidate_actual_path_validation.v1",
        "report_type": "refined_candidate_actual_path_validation",
        "artifact_role": ARTIFACT_ROLE,
        "title": "Refined Candidate Actual-Path Validation",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "research_only": True,
        "summary_status": summary.get("status"),
        **_safety_fields(),
        **_recommendation_safety_fields(),
        "owner_review_required": False,
        **_boundary_fields(),
    }


def _artifact_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "refined_candidate_actual_path_validation_summary.json",
        "actual_path_matrix_json": output_dir / "refined_candidate_actual_path_matrix.json",
        "actual_path_matrix_csv": output_dir / "refined_candidate_actual_path_matrix.csv",
        "prediction_outcome_matrix_json": output_dir
        / "refined_candidate_prediction_outcome_matrix.json",
        "prediction_outcome_matrix_csv": output_dir
        / "refined_candidate_prediction_outcome_matrix.csv",
        "scorecard": output_dir / "refined_candidate_validation_scorecard.json",
        "high_conviction_json": output_dir / "refined_high_conviction_outcome_drilldown.json",
        "high_conviction_csv": output_dir / "refined_high_conviction_outcome_drilldown.csv",
        "false_signal_cost_json": output_dir / "refined_false_signal_cost_matrix.json",
        "false_signal_cost_csv": output_dir / "refined_false_signal_cost_matrix.csv",
        "guardrail_json": output_dir / "refined_guardrail_validation_matrix.json",
        "guardrail_csv": output_dir / "refined_guardrail_validation_matrix.csv",
        "comparison_json": output_dir / "original_vs_refined_actual_path_comparison.json",
        "comparison_csv": output_dir / "original_vs_refined_actual_path_comparison.csv",
        "state_recommendation_matrix": output_dir
        / "refined_candidate_state_recommendation_matrix.json",
        "error_attribution_seed": output_dir / "refined_candidate_error_attribution_seed.json",
        "data_quality_report": output_dir / "refined_candidate_data_quality_report.json",
        "validation_report_doc": docs_root / "refined_candidate_actual_path_validation_report.md",
        "high_conviction_doc": docs_root / "refined_high_conviction_outcome_drilldown.md",
        "comparison_doc": docs_root / "original_vs_refined_actual_path_comparison.md",
        "state_doc": docs_root / "refined_candidate_state_recommendation.md",
    }


def _refined_payload_contract_errors(scope: str, payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    required_fields = (
        "refined_candidate_id",
        "original_candidate_id",
        "candidate_id",
        "source_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "valid_from",
        "valid_until",
        "horizon",
        "provenance",
    )
    for field in required_fields:
        if _is_missing(payload.get(field)):
            errors.append(f"{scope}: missing {field}")
    if payload.get("refined_candidate_id") == payload.get("original_candidate_id"):
        errors.append(f"{scope}: refined_candidate_id must differ from original_candidate_id")
    if payload.get("candidate_id") != payload.get("refined_candidate_id"):
        errors.append(f"{scope}: candidate_id must equal refined_candidate_id")
    if not _strings(payload.get("selected_parameter_set_ids")):
        errors.append(f"{scope}: missing selected_parameter_set_ids")
    provenance = _mapping_from_maybe_json(payload.get("provenance"))
    refinement_source = _mapping_from_maybe_json(provenance.get("refinement_source"))
    if refinement_source.get("task_id") != "TRADING-2287":
        errors.append(f"{scope}: missing refinement_source.task_id")
    if _bool(payload.get("promotion_allowed")) is not False:
        errors.append(f"{scope}: promotion_allowed must be false")
    if _bool(payload.get("paper_shadow_allowed")) is not False:
        errors.append(f"{scope}: paper_shadow_allowed must be false")
    if _bool(payload.get("production_allowed")) is not False:
        errors.append(f"{scope}: production_allowed must be false")
    if str(payload.get("broker_action") or "") != "none":
        errors.append(f"{scope}: broker_action=none required")
    if _bool(payload.get("actual_path_validation_ready")) is not False:
        errors.append(f"{scope}: actual_path_validation_ready must be false")
    if _bool(payload.get("promotion_eligible")) is not False:
        errors.append(f"{scope}: promotion_eligible must be false")
    if _bool(payload.get("permanently_inconclusive_override_allowed")) is not False:
        errors.append(
            f"{scope}: permanently_inconclusive_override_allowed must be false"
        )
    return errors


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        raise RefinedCandidateActualPathValidationError(f"{path}: expected JSON object")
    return dict(payload)


def _read_signal_series_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    for row in rows:
        for key, value in list(row.items()):
            if isinstance(value, str) and value.strip().startswith(("{", "[")):
                try:
                    row[key] = json.loads(value)
                except json.JSONDecodeError:
                    pass
    return rows


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([clean_for_yaml(dict(row)) for row in rows]).to_csv(path, index=False)


def _render_validation_report(
    summary: Mapping[str, Any],
    scorecards: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Refined Candidate Actual-Path Validation Report",
        "",
        "最后更新：2026-06-30",
        "",
        "## Summary",
        "",
        f"- status: `{summary.get('status')}`",
        f"- market_regime: `{summary.get('market_regime')}`",
        f"- candidate_count: `{summary.get('candidate_count')}`",
        f"- actual_path_record_count: `{summary.get('actual_path_record_count')}`",
        f"- validation_eligible_record_count: `{summary.get('validation_eligible_record_count')}`",
        f"- data_quality_status: `{summary.get('data_quality_status')}`",
        f"- guardrail_status_counts: `{summary.get('guardrail_status_counts')}`",
        f"- comparison_label_counts: `{summary.get('comparison_label_counts')}`",
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "## Candidate Scorecards",
        "",
        "|refined_candidate_id|eligible|alignment_rate|confidence_weighted|high_conviction_alignment|guardrail_status|recommended_status|",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for row in scorecards:
        lines.append(
            "|`{}`|{}|{}|{}|{}|`{}`|`{}`|".format(
                row.get("refined_candidate_id"),
                row.get("validation_eligible_record_count"),
                row.get("alignment_rate"),
                row.get("confidence_weighted_alignment_score"),
                row.get("high_conviction_alignment_rate"),
                row.get("guardrail_status"),
                row.get("recommended_research_status"),
            )
        )
    lines.extend(
        [
            "",
            "TRADING-2289 只验证 refined actual-path evidence。high-confidence ratio "
            "提升不等于策略有效，必须同时检查 high-conviction alignment、false "
            "risk-on / false risk-off cost 和 guardrails。",
            "",
            "即使出现 owner review candidate recommendation，也只是进入 owner review，"
            "不是 promotion、paper-shadow、production 或 broker readiness。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_high_conviction_doc(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Refined High-Conviction Outcome Drilldown",
        "",
        "最后更新：2026-06-30",
        "",
        "|refined_candidate_id|asset|horizon|high_eligible|high_alignment|non_high_alignment|delta|label|",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "|`{}`|`{}`|`{}`|{}|{}|{}|{}|`{}`|".format(
                row.get("refined_candidate_id"),
                row.get("target_asset"),
                row.get("horizon"),
                row.get("high_conviction_eligible_count"),
                row.get("high_conviction_alignment_rate"),
                row.get("non_high_conviction_alignment_rate"),
                row.get("high_vs_non_high_alignment_delta"),
                row.get("high_conviction_outcome_label"),
            )
        )
    lines.extend(
        [
            "",
            "该 drilldown 用于判断 refined confidence scaling 是否只是放大噪音；"
            "promotion、paper-shadow、production 和 broker action 继续全部禁止。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_comparison_doc(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Original vs Refined Actual-Path Comparison",
        "",
        "最后更新：2026-06-30",
        "",
        "|original_candidate_id|refined_candidate_id|alignment_delta|confidence_weighted_delta|false_on_delta|false_off_delta|guardrail|label|",
        "|---|---|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "|`{}`|`{}`|{}|{}|{}|{}|`{}`|`{}`|".format(
                row.get("original_candidate_id"),
                row.get("refined_candidate_id"),
                row.get("alignment_rate_delta"),
                row.get("confidence_weighted_score_delta"),
                row.get("false_risk_on_cost_delta"),
                row.get("false_risk_off_cost_delta"),
                row.get("guardrail_status"),
                row.get("comparison_label"),
            )
        )
    lines.extend(
        [
            "",
            "Comparison 只回答 actual-path evidence 是否改善；不改变 TRADING-2285 "
            "original inconclusive 结论，也不产生 promotion readiness。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_state_doc(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Refined Candidate State Recommendation",
        "",
        "最后更新：2026-06-30",
        "",
        "|refined_candidate_id|status|owner_review_candidate|guardrail|comparison|",
        "|---|---|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "|`{}`|`{}`|{}|`{}`|`{}`|".format(
                row.get("refined_candidate_id"),
                row.get("recommended_research_status"),
                row.get("owner_review_candidate_recommendation"),
                row.get("guardrail_status"),
                row.get("comparison_label"),
            )
        )
    lines.extend(
        [
            "",
            "允许状态只限 refined research recommendation。禁止输出 "
            "`PROMOTION_READY`、`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 "
            "`BROKER_READY`。",
            "",
        ]
    )
    return "\n".join(lines)


def _refined_prediction_record(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("signal_confidence") in (None, ""):
        record["signal_confidence"] = record.get("refined_signal_confidence")
    if record.get("signal_value") in (None, ""):
        record["signal_value"] = record.get("refined_signal_value")
    if record.get("refined_candidate_id") in (None, ""):
        record["refined_candidate_id"] = record.get("candidate_id")
    return record


def _scorecards_from_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("candidate_scorecards")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _candidate_rows_from_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("candidate_rows")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _rows_from_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _candidate_rows(rows: Sequence[Mapping[str, Any]], candidate_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if str(row.get("refined_candidate_id") or row.get("candidate_id")) == candidate_id
    ]


def _rows_by_key(
    rows: Sequence[Mapping[str, Any]],
    key: str,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key))].append(dict(row))
    return grouped


def _guardrail_policy_by_original_candidate(
    guardrail_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in guardrail_rows:
        grouped[str(row.get("candidate_id"))].append(row)
    policies: dict[str, dict[str, Any]] = {}
    for candidate_id, rows in grouped.items():
        if not candidate_id or candidate_id == "None":
            continue
        policies[candidate_id] = {
            "candidate_id": candidate_id,
            "guardrail_profile": ",".join(
                sorted(
                    {
                        str(row.get("guardrail_profile"))
                        for row in rows
                        if row.get("guardrail_profile")
                    }
                )
            ),
            "max_high_confidence_ratio": min(
                to_float(row.get("max_high_confidence_ratio"), default=1.0) for row in rows
            ),
            "max_false_risk_on_cost_increase": min(
                to_float(row.get("max_false_risk_on_cost_increase")) for row in rows
            ),
            "max_false_risk_off_cost_increase": min(
                to_float(row.get("max_false_risk_off_cost_increase")) for row in rows
            ),
            "max_neutral_ratio_reduction": min(
                to_float(row.get("max_neutral_ratio_reduction"), default=1.0) for row in rows
            ),
            "minimum_eligible_records": max(
                int(to_float(row.get("minimum_eligible_records"))) for row in rows
            ),
            "data_quality_required_status": sorted(
                {
                    status
                    for row in rows
                    for status in _strings(row.get("data_quality_required_status"))
                }
            ),
        }
    return policies


def _empty_guardrail_metrics(
    comparison: Mapping[str, Any],
    score: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    return {
        "max_high_confidence_ratio": None,
        "actual_high_confidence_ratio": comparison.get("high_confidence_ratio_refined"),
        "max_false_risk_on_cost_increase": None,
        "actual_false_risk_on_cost_delta": comparison.get("false_risk_on_cost_delta"),
        "max_false_risk_off_cost_increase": None,
        "actual_false_risk_off_cost_delta": comparison.get("false_risk_off_cost_delta"),
        "max_neutral_ratio_reduction": None,
        "actual_neutral_ratio_reduction": round_float(
            max(
                to_float(comparison.get("neutral_ratio_original"))
                - to_float(comparison.get("neutral_ratio_refined")),
                0.0,
            )
        ),
        "minimum_eligible_records": None,
        "actual_eligible_records": int(score.get("validation_eligible_record_count") or 0),
        "data_quality_required_status": [],
        "actual_data_quality_status": data_quality_status,
    }


def _assert_input_payload_safe(scope: str, payload: Any) -> None:
    for path, item in _walk_payload(payload):
        label = f"{scope}{path}"
        if isinstance(item, Mapping):
            for field in ("promotion_allowed", "paper_shadow_allowed", "production_allowed"):
                if field in item and _bool(item.get(field)) is not False:
                    raise RefinedCandidateActualPathValidationError(
                        f"{label}: {field} must be false"
                    )
            if "broker_action" in item and str(item.get("broker_action") or "") != "none":
                raise RefinedCandidateActualPathValidationError(
                    f"{label}: broker_action must be none"
                )


def _assert_generated_payload_safe(scope: str, payload: Any) -> None:
    _assert_input_payload_safe(scope, payload)
    _assert_no_banned_recommendations(scope, payload)


def _assert_no_banned_recommendations(scope: str, payload: Any) -> None:
    for path, item in _walk_payload(payload):
        if isinstance(item, str) and item in BANNED_RECOMMENDATIONS:
            raise RefinedCandidateActualPathValidationError(
                f"{scope}{path}: banned readiness recommendation {item}"
            )


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
        raise RefinedCandidateActualPathValidationError("input list must be non-empty")
    return parsed


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return tuple(str(item).strip() for item in parsed if str(item).strip())
        return tuple(item.strip() for item in stripped.split(",") if item.strip())
    if isinstance(value, list | tuple | set):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def _mapping_from_maybe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


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
    if value is None:
        return True
    return isinstance(value, str) and not value.strip()


def _eligible_rows(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [row for row in rows if bool(row.get("validation_eligible"))]


def _alignment_rate(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    return sum(1 for row in rows if to_float(row.get("alignment_score")) > 0.0) / len(rows)


def _mean(values: Sequence[float]) -> float:
    clean = [value for value in values if math.isfinite(value)]
    return sum(clean) / len(clean) if clean else 0.0


def _weighted_mean(values: Sequence[float], weights: Sequence[float]) -> float:
    clean = [
        (value, weight)
        for value, weight in zip(values, weights, strict=True)
        if math.isfinite(value) and math.isfinite(weight) and weight > 0.0
    ]
    total_weight = sum(weight for _, weight in clean)
    if total_weight <= 0.0:
        return 0.0
    return sum(value * weight for value, weight in clean) / total_weight


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


def _neutral_ratio(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    return sum(1 for row in rows if str(row.get("signal_direction")) == "neutral") / len(rows)


def _high_confidence_ratio(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    if any("high_conviction_flag" in row for row in rows):
        return sum(1 for row in rows if _bool(row.get("high_conviction_flag"))) / len(rows)
    return (
        sum(
            1
            for row in rows
            if to_float(row.get("signal_confidence")) >= HIGH_CONFIDENCE_NEUTRAL_THRESHOLD
        )
        / len(rows)
    )


def _error_cost(rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        abs(to_float(row.get("actual_forward_return")))
        + abs(to_float(row.get("actual_max_drawdown")))
        for row in rows
    )


def _comparison_label(
    *,
    confidence_weighted_score_delta: float,
    alignment_rate_delta: float,
    false_risk_on_cost_delta: float,
    false_risk_off_cost_delta: float,
    high_conviction_alignment_rate_refined: float,
    alignment_rate_original: float,
    guardrail_status: str,
    data_quality_status_refined: str,
) -> str:
    if data_quality_status_refined == "FAIL":
        return "DATA_QUALITY_BLOCKED"
    if (
        abs(confidence_weighted_score_delta) < COMPARISON_MATERIALITY_THRESHOLD
        and false_risk_on_cost_delta >= 0.0
        and false_risk_off_cost_delta >= 0.0
    ):
        return "REFINED_NO_MEASURABLE_IMPROVEMENT"
    if (
        confidence_weighted_score_delta > 0.0
        and alignment_rate_delta >= 0.0
        and false_risk_on_cost_delta <= 0.0
        and false_risk_off_cost_delta <= 0.0
        and guardrail_status == "PASS"
        and data_quality_status_refined in {"PASS", "PASS_WITH_WARNINGS"}
    ):
        return "REFINED_IMPROVED_WITHIN_GUARDRAILS"
    if confidence_weighted_score_delta > 0.0 and (
        false_risk_on_cost_delta > 0.0 or false_risk_off_cost_delta > 0.0
    ):
        return "REFINED_IMPROVED_BUT_FALSE_COST_WORSE"
    if (
        high_conviction_alignment_rate_refined > alignment_rate_original
        and alignment_rate_delta <= 0.0
    ):
        return "REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED"
    if (
        confidence_weighted_score_delta < 0.0
        or alignment_rate_delta < 0.0
        or false_risk_on_cost_delta > 0.0
        or false_risk_off_cost_delta > 0.0
    ):
        return "REFINED_WORSE"
    return "REFINED_INCONCLUSIVE"


def _high_conviction_label(
    *,
    high_eligible_count: int,
    alignment_delta: float,
    false_cost_delta: float,
    minimum_high_conviction_records: int,
) -> str:
    if high_eligible_count < minimum_high_conviction_records:
        return "HIGH_CONVICTION_INSUFFICIENT_SAMPLE"
    if false_cost_delta > 0.0:
        return "HIGH_CONVICTION_FALSE_COST_WORSE"
    if alignment_delta > 0.0 and false_cost_delta <= 0.0:
        return "HIGH_CONVICTION_EDGE_IMPROVED"
    return "HIGH_CONVICTION_NO_EDGE"


def _state_recommendation(
    *,
    comparison_label: str,
    guardrail_status: str,
    data_quality_status: str,
    eligible_count: int,
    high_conviction_labels: set[str],
) -> str:
    if data_quality_status == "FAIL" or comparison_label == "DATA_QUALITY_BLOCKED":
        return "REFINED_DATA_QUALITY_BLOCKED"
    if guardrail_status == "FAIL":
        return "REFINED_GUARDRAIL_BLOCKED"
    if eligible_count < MINIMUM_ELIGIBLE_RECORDS_FOR_REFINED_REVIEW:
        return "REFINED_ACTUAL_PATH_VALIDATED_INCONCLUSIVE"
    if (
        comparison_label == "REFINED_IMPROVED_WITHIN_GUARDRAILS"
        and "HIGH_CONVICTION_EDGE_IMPROVED" in high_conviction_labels
        and "HIGH_CONVICTION_FALSE_COST_WORSE" not in high_conviction_labels
    ):
        return "REFINED_ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE"
    if comparison_label in {
        "REFINED_IMPROVED_WITHIN_GUARDRAILS",
        "REFINED_IMPROVED_BUT_FALSE_COST_WORSE",
        "REFINED_HIGH_CONFIDENCE_ONLY_IMPROVED",
    }:
        return "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"
    if comparison_label == "REFINED_WORSE":
        return "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
    return "REFINED_ACTUAL_PATH_VALIDATED_INCONCLUSIVE"


def _preliminary_refined_research_status(
    *,
    record_count: int,
    eligible_count: int,
    confidence_weighted_score: float,
) -> str:
    if record_count == 0:
        return "REFINED_DATA_QUALITY_BLOCKED"
    if eligible_count < MINIMUM_ELIGIBLE_RECORDS_FOR_REFINED_REVIEW:
        return "REFINED_ACTUAL_PATH_VALIDATED_INCONCLUSIVE"
    if confidence_weighted_score < -COMPARISON_MATERIALITY_THRESHOLD:
        return "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
    return "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"


def _state_owner_review_candidate(status: str) -> bool:
    return status == "REFINED_ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE"


def _dominant_observed_driver(actual_path: Mapping[str, Any]) -> str:
    if not bool(actual_path.get("validation_eligible")):
        return "data_gap"
    forward_return = to_float(actual_path.get("forward_return"))
    max_drawdown = to_float(actual_path.get("max_drawdown_during_horizon"))
    realized_vol = to_float(actual_path.get("realized_volatility"))
    if forward_return >= 0.04:
        return "sharp_rebound"
    if max_drawdown <= -0.05:
        return "drawdown_event"
    if realized_vol >= 0.25:
        return "volatility_expansion"
    if realized_vol <= 0.18:
        return "volatility_compression"
    if forward_return > 0.005:
        return "trend_continuation"
    if forward_return < -0.005:
        return "trend_reversal"
    return "noisy_path"


def _owner_review_note(error_type: str) -> str:
    if error_type == "no_error":
        return "No initial refined error attribution required."
    if error_type == "data_quality_inconclusive":
        return (
            "Refined actual-path evidence is ineligible or coverage-limited; "
            "review data coverage first."
        )
    return f"Initial TRADING-2289 refined seed classification: {error_type}."


def _record_id(record: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(record.get("refined_candidate_id") or record.get("candidate_id")),
            str(record.get("original_candidate_id")),
            str(record.get("target_asset")),
            str(record.get("horizon")),
            str(record.get("signal_name")),
            str(record.get("decision_timestamp")),
            str(record.get("source_row_index", "")),
        ]
    )


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
        "trading_2285_original_inconclusive_decisions_changed": False,
    }
