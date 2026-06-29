from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REFINEMENT_PLAN_ROOT,
)
from ai_trading_system.candidate_signal_binding_schema import (
    PREDICTION_SCHEMA_VERSION,
    SIGNAL_SPEC_VERSION,
    candidate_bound_signal_series_contract_dict,
)
from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_ORIGINAL_GENERATOR_ROOT,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    candidate_artifact_safety_fields,
    generator_operation_safety_fields,
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    round_float,
    to_float,
    write_json,
    write_markdown,
)
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "refined_candidate_generators_regenerated"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2288_REFINED_CANDIDATE_REGENERATION_WITH_ADJUSTED_CONFIDENCE_SCALING"
STATUS = "REFINED_CANDIDATE_ARTIFACTS_READY_REFINED_ACTUAL_PATH_VALIDATION_BLOCKED"
MODE = "refined_regeneration"
REFINEMENT_SOURCE_TASK = "TRADING-2287"
REFINEMENT_VERSION = "refined_confidence_v1"
REFINED_ARTIFACT_ROLE = "refined_regenerated_executable_candidate_artifact"
NEXT_TASK = "TRADING-2289_Refined_Candidate_Actual_Path_Validation"
DEFAULT_CANDIDATES = (
    "baseline_plus_trend_structure",
    "risk_appetite",
    "volatility_regime",
)
REFINED_CANDIDATE_IDS = {
    "baseline_plus_trend_structure": (
        "baseline_plus_trend_structure_refined_confidence_v1"
    ),
    "risk_appetite": "risk_appetite_refined_confidence_v1",
    "volatility_regime": "volatility_regime_refined_confidence_v1",
}

REQUIRED_REFINEMENT_PLAN_FILES = {
    "summary": "confidence_scaling_refinement_summary.json",
    "proposals": "candidate_confidence_scaling_proposal_matrix.json",
    "parameter_grid": "candidate_confidence_scaling_parameter_grid.json",
    "guardrails": "candidate_guardrail_matrix.json",
    "implementation_plan": "candidate_2288_implementation_plan.json",
}
REQUIRED_ORIGINAL_CANDIDATE_FILES = {
    "signal_spec": "candidate_signal_spec.json",
    "signal_series": "candidate_signal_series.csv",
    "prediction_artifact": "candidate_prediction_artifact.json",
    "generation_summary": "generation_summary.json",
    "validation_summary": "validation_summary.json",
}
BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}

# TRADING-2288 deterministic refinement constants. These are research-only
# artifact-generation mechanics documented in docs/requirements/TRADING-2288_*.md,
# not promotion gates or investment acceptance thresholds.
HIGH_CONVICTION_RANKING_EPSILON = 1e-12
NEUTRAL_HIGH_CONFIDENCE_MARGIN = 0.01
MAX_SELECTED_PARAMETER_SETS_PER_CANDIDATE = 3
LOW_CONFIDENCE_THRESHOLD_DEFAULT = 0.35


class RefinedCandidateRegenerationError(ValueError):
    pass


@dataclass(frozen=True)
class OriginalCandidateArtifacts:
    candidate_id: str
    refined_candidate_id: str
    candidate_dir: Path
    signal_spec_path: Path
    signal_series_path: Path
    prediction_artifact_path: Path
    generation_summary_path: Path
    validation_summary_path: Path
    signal_spec: dict[str, Any]
    signal_series_rows: list[dict[str, Any]]
    prediction_artifact: dict[str, Any]
    prediction_records: list[dict[str, Any]]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]


@dataclass(frozen=True)
class ParameterApplication:
    candidate_id: str
    refined_candidate_id: str
    selected_proposal_ids: tuple[str, ...]
    selected_parameter_set_ids: tuple[str, ...]
    rejected_parameter_set_ids: tuple[str, ...]
    rejection_reasons: dict[str, str]
    applied_neutral_band_width: float
    applied_confidence_scale_factor: float
    applied_confidence_cap: float
    applied_confidence_floor: float
    applied_high_confidence_threshold: float
    applied_low_confidence_threshold: float
    applied_directional_activation_threshold: float
    applied_missing_input_penalty: float
    expected_high_confidence_ratio: float
    expected_directional_signal_ratio: float
    max_high_confidence_ratio: float
    guardrail_profile: str
    guardrail_compliant: bool


@dataclass(frozen=True)
class RefinedCandidateInputs:
    refinement_plan_dir: Path
    original_generator_dir: Path
    summary: dict[str, Any]
    proposal_rows: list[dict[str, Any]]
    parameter_grid_rows: list[dict[str, Any]]
    guardrail_rows: list[dict[str, Any]]
    implementation_rows: list[dict[str, Any]]
    original_artifacts: dict[str, OriginalCandidateArtifacts]


def run_refined_candidate_generators_regenerate(
    *,
    refinement_plan_dir: Path = DEFAULT_REFINEMENT_PLAN_ROOT,
    original_generator_dir: Path = DEFAULT_ORIGINAL_GENERATOR_ROOT,
    candidates: Sequence[str] | str = DEFAULT_CANDIDATES,
    target_assets: Sequence[str] | str = ("QQQ", "SPY", "SMH"),
    horizons: Sequence[str] | str = ("5d", "10d", "20d"),
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise RefinedCandidateRegenerationError(
            "refined candidate regeneration only supports refined_regeneration mode"
        )
    candidate_ids = _normalize_list(candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    inputs = load_refined_candidate_regeneration_inputs(
        refinement_plan_dir=refinement_plan_dir,
        original_generator_dir=original_generator_dir,
        candidates=candidate_ids,
    )

    candidate_results: list[dict[str, Any]] = []
    validation_summaries: list[dict[str, Any]] = []
    delta_rows: list[dict[str, Any]] = []
    selected_proposal_ids: set[str] = set()
    selected_parameter_set_ids: set[str] = set()

    for candidate_id in candidate_ids:
        original = inputs.original_artifacts[candidate_id]
        application = select_refined_parameter_application(
            candidate_id=candidate_id,
            inputs=inputs,
        )
        selected_proposal_ids.update(application.selected_proposal_ids)
        selected_parameter_set_ids.update(application.selected_parameter_set_ids)

        original_records = _filter_records(
            original.prediction_records,
            target_assets=asset_ids,
            horizons=horizon_ids,
        )
        if not original_records:
            raise RefinedCandidateRegenerationError(
                f"{candidate_id} has no original records for requested assets/horizons"
            )
        source_hash = _stable_hash(
            {
                "original_candidate_id": candidate_id,
                "refined_candidate_id": application.refined_candidate_id,
                "refinement_plan_summary": _sha256(
                    refinement_plan_dir / REQUIRED_REFINEMENT_PLAN_FILES["summary"]
                ),
                "original_prediction_artifact": _sha256(original.prediction_artifact_path),
                "selected_parameter_set_ids": application.selected_parameter_set_ids,
            }
        )
        provenance_source_paths, provenance_source_hashes = _refined_source_lineage(
            original=original,
            refinement_plan_dir=refinement_plan_dir,
        )
        input_snapshot_hash = _stable_hash(
            {
                "snapshot": "refined_input",
                "original_input_snapshot_hash": original.prediction_artifact.get(
                    "input_snapshot_hash"
                ),
                "candidate_id": candidate_id,
                "refined_candidate_id": application.refined_candidate_id,
                "selected_parameter_set_ids": application.selected_parameter_set_ids,
            }
        )
        feature_snapshot_hash = _stable_hash(
            {
                "snapshot": "refined_feature",
                "original_feature_snapshot_hash": original.prediction_artifact.get(
                    "feature_snapshot_hash"
                ),
                "selected_proposal_ids": application.selected_proposal_ids,
                "selected_parameter_set_ids": application.selected_parameter_set_ids,
            }
        )
        refined_records = apply_refined_confidence_scaling(
            original_records=original_records,
            original=original,
            application=application,
            provenance_source_paths=provenance_source_paths,
            provenance_source_hashes=provenance_source_hashes,
            generated_at=generated_at,
            source_artifact_hash=source_hash,
            input_snapshot_hash=input_snapshot_hash,
            feature_snapshot_hash=feature_snapshot_hash,
        )
        refined_spec = build_refined_candidate_signal_spec(
            original.signal_spec,
            application=application,
        )
        refined_artifact = build_refined_candidate_prediction_artifact(
            original=original,
            refined_records=refined_records,
            refined_spec=refined_spec,
            application=application,
            provenance_source_paths=provenance_source_paths,
            provenance_source_hashes=provenance_source_hashes,
            generated_at=generated_at,
            source_artifact_hash=source_hash,
            input_snapshot_hash=input_snapshot_hash,
            feature_snapshot_hash=feature_snapshot_hash,
        )
        validation_summary = validate_refined_candidate_artifacts(
            refined_signal_spec=refined_spec,
            refined_signal_records=refined_records,
            refined_prediction_artifact=refined_artifact,
            application=application,
        )
        validation_summaries.append(validation_summary)
        if validation_summary["status"] != "PASS":
            raise RefinedCandidateRegenerationError(
                f"{candidate_id} refined validation failed: "
                + "; ".join(str(error) for error in validation_summary["errors"])
            )

        candidate_dir = output_dir / application.refined_candidate_id
        paths = _candidate_output_paths(candidate_dir)
        parameter_report = build_refined_parameter_application_report(application)
        delta = build_refined_original_vs_refined_delta(
            original_candidate_id=candidate_id,
            refined_candidate_id=application.refined_candidate_id,
            original_records=original_records,
            refined_records=refined_records,
            application=application,
        )
        delta_rows.append(delta)
        generation_summary = build_refined_generation_summary(
            original=original,
            refined_signal_records=refined_records,
            refined_prediction_artifact=refined_artifact,
            validation_summary=validation_summary,
            parameter_report=parameter_report,
            delta=delta,
            generated_at=generated_at,
            paths=paths,
        )

        write_json(paths["refined_candidate_signal_spec_json"], refined_spec)
        write_refined_signal_series_csv(
            paths["refined_candidate_signal_series_csv"],
            refined_records,
        )
        write_json(
            paths["refined_candidate_prediction_artifact_json"],
            refined_artifact,
        )
        write_json(paths["refined_generation_summary_json"], generation_summary)
        write_json(paths["refined_validation_summary_json"], validation_summary)
        write_json(paths["refined_parameter_application_report_json"], parameter_report)
        write_json(paths["refined_original_vs_refined_delta_json"], delta)

        candidate_results.append(
            {
                "original_candidate_id": candidate_id,
                "refined_candidate_id": application.refined_candidate_id,
                "selected_proposal_ids": list(application.selected_proposal_ids),
                "selected_parameter_set_ids": list(application.selected_parameter_set_ids),
                "artifact_paths": {key: str(path) for key, path in paths.items()},
                "generation_summary": generation_summary,
                "validation_summary": validation_summary,
                "parameter_application_report": parameter_report,
                "original_vs_refined_delta": delta,
            }
        )

    top_validation = build_refined_top_level_validation_summary(
        candidate_results=candidate_results,
        validation_summaries=validation_summaries,
        generated_at=generated_at,
    )
    run_summary = build_refined_regeneration_run_summary(
        inputs=inputs,
        candidate_ids=candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        selected_scaling_proposal_count=len(selected_proposal_ids),
        selected_parameter_set_count=len(selected_parameter_set_ids),
        candidate_results=candidate_results,
        validation_summary=top_validation,
        generated_at=generated_at,
        mode=mode,
    )
    delta_summary = build_refined_original_vs_refined_delta_summary(
        delta_rows=delta_rows,
        generated_at=generated_at,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "refined_regeneration_run_summary.json", run_summary)
    write_json(output_dir / "refined_regeneration_validation_summary.json", top_validation)
    write_json(
        output_dir / "refined_original_vs_refined_delta_summary.json",
        delta_summary,
    )
    _write_docs(
        docs_root=docs_root,
        run_summary=run_summary,
        candidate_results=candidate_results,
        delta_rows=delta_rows,
    )

    payload = {
        "schema_version": "refined_candidate_regeneration_result.v1",
        "report_type": "refined_candidate_regeneration",
        "title": "Refined Candidate Regeneration with Adjusted Confidence Scaling",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "summary": run_summary,
        "candidate_results": candidate_results,
        "validation_summary": top_validation,
        "delta_summary": delta_summary,
        "artifact_paths": {
            "refined_regeneration_run_summary_json": str(
                output_dir / "refined_regeneration_run_summary.json"
            ),
            "refined_regeneration_validation_summary_json": str(
                output_dir / "refined_regeneration_validation_summary.json"
            ),
            "refined_original_vs_refined_delta_summary_json": str(
                output_dir / "refined_original_vs_refined_delta_summary.json"
            ),
            "refined_candidate_regeneration_report_md": str(
                docs_root / "refined_candidate_regeneration_report.md"
            ),
            "refined_candidate_parameter_application_report_md": str(
                docs_root / "refined_candidate_parameter_application_report.md"
            ),
            "refined_original_vs_refined_delta_summary_md": str(
                docs_root / "refined_original_vs_refined_delta_summary.md"
            ),
        },
        "research_only": True,
        "dynamic_promotion_status": "BLOCKED",
        "regeneration_executed": True,
        "actual_path_validation_executed": False,
        "actual_path_validation_ready": False,
        "historical_executable_artifact": True,
        **candidate_artifact_safety_fields(),
        **_operation_boundary_fields(),
    }
    _assert_generated_payload_safe("refined_candidate_regeneration_result", payload)
    return clean_for_yaml(payload)


def load_refined_candidate_regeneration_inputs(
    *,
    refinement_plan_dir: Path,
    original_generator_dir: Path,
    candidates: Sequence[str] | str,
) -> RefinedCandidateInputs:
    candidate_ids = _normalize_list(candidates)
    plan_paths = {
        name: refinement_plan_dir / filename
        for name, filename in REQUIRED_REFINEMENT_PLAN_FILES.items()
    }
    missing = [str(path) for path in plan_paths.values() if not path.exists()]
    if missing:
        raise RefinedCandidateRegenerationError(
            f"missing refined regeneration input artifact(s): {missing}"
        )

    plan_payloads = {name: _read_json(path) for name, path in plan_paths.items()}
    for name, payload in plan_payloads.items():
        _assert_input_payload_safe(f"refinement_plan.{name}", payload)
        _assert_no_banned_recommendations(f"refinement_plan.{name}", payload)
    summary = dict(plan_payloads["summary"].get("summary") or {})
    if _bool(summary.get("regeneration_executed")):
        raise RefinedCandidateRegenerationError(
            "refinement plan input must have regeneration_executed=false"
        )
    if _bool(summary.get("actual_path_validation_executed")):
        raise RefinedCandidateRegenerationError(
            "refinement plan input must have actual_path_validation_executed=false"
        )

    proposal_rows = _rows_from_payload(plan_payloads["proposals"])
    parameter_grid_rows = _rows_from_payload(plan_payloads["parameter_grid"])
    guardrail_rows = _rows_from_payload(plan_payloads["guardrails"])
    implementation_rows = _rows_from_payload(plan_payloads["implementation_plan"])
    for row_name, rows in (
        ("proposal_rows", proposal_rows),
        ("parameter_grid_rows", parameter_grid_rows),
        ("guardrail_rows", guardrail_rows),
        ("implementation_rows", implementation_rows),
    ):
        if not rows:
            raise RefinedCandidateRegenerationError(f"{row_name} is empty")
        for index, row in enumerate(rows):
            _assert_input_payload_safe(f"{row_name}[{index}]", row)
            _assert_no_banned_recommendations(f"{row_name}[{index}]", row)

    original_artifacts = {
        candidate_id: _load_original_candidate_artifacts(
            original_generator_dir=original_generator_dir,
            candidate_id=candidate_id,
        )
        for candidate_id in candidate_ids
    }
    return RefinedCandidateInputs(
        refinement_plan_dir=refinement_plan_dir,
        original_generator_dir=original_generator_dir,
        summary=summary,
        proposal_rows=proposal_rows,
        parameter_grid_rows=parameter_grid_rows,
        guardrail_rows=guardrail_rows,
        implementation_rows=implementation_rows,
        original_artifacts=original_artifacts,
    )


def select_refined_parameter_application(
    *,
    candidate_id: str,
    inputs: RefinedCandidateInputs,
) -> ParameterApplication:
    refined_candidate_id = _refined_candidate_id(candidate_id)
    implementation = _single_row(
        inputs.implementation_rows,
        candidate_id=candidate_id,
        label="implementation_plan",
    )
    selected_proposal_ids = tuple(_strings(implementation.get("selected_proposal_ids")))
    selected_parameter_set_ids = tuple(_strings(implementation.get("selected_parameter_set_ids")))
    if not selected_proposal_ids:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} selected_proposal_ids is empty"
        )
    if not selected_parameter_set_ids:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} selected_parameter_set_ids is empty"
        )
    if len(selected_parameter_set_ids) > MAX_SELECTED_PARAMETER_SETS_PER_CANDIDATE:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} selected more than "
            f"{MAX_SELECTED_PARAMETER_SETS_PER_CANDIDATE} parameter sets"
        )

    selected_grid_rows = [
        row
        for row in inputs.parameter_grid_rows
        if str(row.get("candidate_id")) == candidate_id
        and str(row.get("parameter_set_id")) in set(selected_parameter_set_ids)
    ]
    if len(selected_grid_rows) != len(selected_parameter_set_ids):
        found = {str(row.get("parameter_set_id")) for row in selected_grid_rows}
        missing = sorted(set(selected_parameter_set_ids) - found)
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} missing selected parameter set(s): {missing}"
        )

    guardrail_by_proposal = {
        str(row.get("proposal_id")): row
        for row in inputs.guardrail_rows
        if str(row.get("candidate_id")) == candidate_id
    }
    rejected: dict[str, str] = {}
    for row in selected_grid_rows:
        proposal_id = str(row.get("proposal_id") or "")
        guardrail = guardrail_by_proposal.get(proposal_id)
        if guardrail is None:
            rejected[str(row.get("parameter_set_id"))] = "missing_guardrail"
            continue
        expected_high = to_float(row.get("expected_high_confidence_ratio"))
        max_high = to_float(guardrail.get("max_high_confidence_ratio"))
        expected_directional = to_float(row.get("expected_directional_signal_ratio"))
        min_directional = to_float(guardrail.get("min_directional_signal_ratio"))
        if expected_high > max_high:
            rejected[str(row.get("parameter_set_id"))] = (
                "expected_high_confidence_ratio_exceeds_guardrail"
            )
        elif expected_directional < min_directional:
            rejected[str(row.get("parameter_set_id"))] = (
                "expected_directional_ratio_below_guardrail"
            )
    if rejected:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} selected parameter set(s) are not guardrail compliant: {rejected}"
        )

    guardrails = [guardrail_by_proposal[str(row.get("proposal_id"))] for row in selected_grid_rows]
    all_candidate_grid_ids = {
        str(row.get("parameter_set_id"))
        for row in inputs.parameter_grid_rows
        if str(row.get("candidate_id")) == candidate_id
    }
    rejected_grid_ids = tuple(sorted(all_candidate_grid_ids - set(selected_parameter_set_ids)))
    rejection_reasons = {
        parameter_set_id: "not_selected_by_2287_implementation_plan"
        for parameter_set_id in rejected_grid_ids
    }
    return ParameterApplication(
        candidate_id=candidate_id,
        refined_candidate_id=refined_candidate_id,
        selected_proposal_ids=selected_proposal_ids,
        selected_parameter_set_ids=selected_parameter_set_ids,
        rejected_parameter_set_ids=rejected_grid_ids,
        rejection_reasons=rejection_reasons,
        applied_neutral_band_width=_mean_field(selected_grid_rows, "neutral_band_width"),
        applied_confidence_scale_factor=_mean_field(
            selected_grid_rows,
            "confidence_scale_factor",
        ),
        applied_confidence_cap=min(
            to_float(row.get("confidence_cap")) for row in selected_grid_rows
        ),
        applied_confidence_floor=max(
            to_float(row.get("confidence_floor")) for row in selected_grid_rows
        ),
        applied_high_confidence_threshold=max(
            to_float(row.get("high_confidence_threshold")) for row in selected_grid_rows
        ),
        applied_low_confidence_threshold=max(
            (
                to_float(row.get("low_confidence_threshold"))
                for row in selected_grid_rows
                if row.get("low_confidence_threshold") is not None
            ),
            default=LOW_CONFIDENCE_THRESHOLD_DEFAULT,
        ),
        applied_directional_activation_threshold=max(
            to_float(row.get("directional_activation_threshold")) for row in selected_grid_rows
        ),
        applied_missing_input_penalty=min(
            to_float(row.get("missing_input_penalty")) for row in selected_grid_rows
        ),
        expected_high_confidence_ratio=_mean_field(
            selected_grid_rows,
            "expected_high_confidence_ratio",
        ),
        expected_directional_signal_ratio=_mean_field(
            selected_grid_rows,
            "expected_directional_signal_ratio",
        ),
        max_high_confidence_ratio=min(
            to_float(row.get("max_high_confidence_ratio")) for row in guardrails
        ),
        guardrail_profile=";".join(
            sorted({str(row.get("guardrail_profile")) for row in selected_grid_rows})
        ),
        guardrail_compliant=True,
    )


def apply_refined_confidence_scaling(
    *,
    original_records: Sequence[Mapping[str, Any]],
    original: OriginalCandidateArtifacts,
    application: ParameterApplication,
    provenance_source_paths: Sequence[str],
    provenance_source_hashes: Sequence[str],
    generated_at: datetime,
    source_artifact_hash: str,
    input_snapshot_hash: str,
    feature_snapshot_hash: str,
) -> list[dict[str, Any]]:
    preliminary: list[dict[str, Any]] = []
    for row in original_records:
        original_value = to_float(row.get("signal_value"))
        original_confidence = to_float(row.get("signal_confidence"))
        refined_value = round_float(max(-1.0, min(1.0, original_value)))
        refined_direction = _refined_direction(
            candidate_id=application.candidate_id,
            signal_name=str(row.get("signal_name") or ""),
            signal_value=refined_value,
            neutral_band_width=application.applied_neutral_band_width,
        )
        refined_confidence = _scaled_confidence(
            original_signal_value=original_value,
            original_signal_confidence=original_confidence,
            refined_signal_direction=refined_direction,
            provenance=_provenance(row),
            application=application,
        )
        direction_changed = refined_direction != str(row.get("signal_direction") or "")
        refined_row = _refined_record_payload(
            row=row,
            original=original,
            application=application,
            provenance_source_paths=provenance_source_paths,
            provenance_source_hashes=provenance_source_hashes,
            generated_at=generated_at,
            source_artifact_hash=source_artifact_hash,
            input_snapshot_hash=input_snapshot_hash,
            feature_snapshot_hash=feature_snapshot_hash,
            refined_signal_value=refined_value,
            refined_signal_direction=refined_direction,
            refined_signal_confidence=refined_confidence,
            direction_changed=direction_changed,
        )
        preliminary.append(refined_row)

    potential = [
        (
            index,
            abs(to_float(row.get("refined_signal_value")))
            * max(to_float(row.get("refined_signal_confidence")), HIGH_CONVICTION_RANKING_EPSILON),
        )
        for index, row in enumerate(preliminary)
        if _high_conviction_base_condition(row, application)
    ]
    max_count = int(math.floor(len(preliminary) * application.max_high_confidence_ratio))
    allowed_indices = {
        index
        for index, _score in sorted(potential, key=lambda item: item[1], reverse=True)[
            :max_count
        ]
    }
    for index, row in enumerate(preliminary):
        high_flag = index in allowed_indices
        if not high_flag and _high_conviction_base_condition(row, application):
            bounded_confidence = round_float(
                max(
                    application.applied_confidence_floor,
                    application.applied_high_confidence_threshold
                    - NEUTRAL_HIGH_CONFIDENCE_MARGIN,
                )
            )
            row["signal_confidence"] = bounded_confidence
            row["refined_signal_confidence"] = bounded_confidence
            row["confidence_delta"] = round_float(
                bounded_confidence - to_float(row.get("original_signal_confidence"))
            )
        row["high_conviction_flag"] = high_flag
        row["source_prediction_flags"] = {
            **_mapping(row.get("source_prediction_flags")),
            "refined_candidate_artifact": True,
            "high_conviction_flag": high_flag,
            "actual_path_validation_ready": False,
        }
        row["prediction_fields"] = {
            **_mapping(row.get("prediction_fields")),
            "candidate_signal": row.get("signal_name"),
            "signal_value": row.get("signal_value"),
            "signal_direction": row.get("signal_direction"),
            "signal_confidence": row.get("signal_confidence"),
            "refined_candidate_artifact": True,
            "high_conviction_flag": high_flag,
            "actual_path_validation_ready": False,
        }
    return [clean_for_yaml(row) for row in preliminary]


def build_refined_candidate_signal_spec(
    original_spec: Mapping[str, Any],
    *,
    application: ParameterApplication,
) -> dict[str, Any]:
    spec = dict(original_spec)
    spec.update(
        {
            "candidate_id": application.refined_candidate_id,
            "original_candidate_id": application.candidate_id,
            "refined_candidate_id": application.refined_candidate_id,
            "generator_id": f"{application.refined_candidate_id}_generator",
            "generator_version": f"{original_spec.get('generator_version')}.{REFINEMENT_VERSION}",
            "refinement_source_task": REFINEMENT_SOURCE_TASK,
            "refinement_task_id": TASK_ID,
            "refinement_version": REFINEMENT_VERSION,
            "confidence_scaling_method": "guardrail_bounded_piecewise_directional_scaling",
            "selected_proposal_ids": list(application.selected_proposal_ids),
            "selected_parameter_set_ids": list(application.selected_parameter_set_ids),
            "artifact_role": "candidate_signal_spec",
            **generator_operation_safety_fields(),
            "promotion_eligible": False,
            "permanently_inconclusive_override_allowed": False,
        }
    )
    return clean_for_yaml(spec)


def build_refined_candidate_prediction_artifact(
    *,
    original: OriginalCandidateArtifacts,
    refined_records: Sequence[Mapping[str, Any]],
    refined_spec: Mapping[str, Any],
    application: ParameterApplication,
    provenance_source_paths: Sequence[str],
    provenance_source_hashes: Sequence[str],
    generated_at: datetime,
    source_artifact_hash: str,
    input_snapshot_hash: str,
    feature_snapshot_hash: str,
) -> dict[str, Any]:
    if not refined_records:
        raise RefinedCandidateRegenerationError("refined prediction artifact requires records")
    latest = dict(refined_records[-1])
    provenance = _refined_provenance(
        original=original,
        row=latest,
        application=application,
        provenance_source_paths=provenance_source_paths,
        provenance_source_hashes=provenance_source_hashes,
    )
    artifact = {
        "schema_version": PREDICTION_SCHEMA_VERSION,
        "artifact_id": f"{application.refined_candidate_id}_prediction_artifact",
        "artifact_role": REFINED_ARTIFACT_ROLE,
        "candidate_id": application.refined_candidate_id,
        "candidate_family": latest["candidate_family"],
        "original_candidate_id": application.candidate_id,
        "refined_candidate_id": application.refined_candidate_id,
        "refinement_source_task": REFINEMENT_SOURCE_TASK,
        "refinement_task_id": TASK_ID,
        "refinement_version": REFINEMENT_VERSION,
        "selected_proposal_ids": list(application.selected_proposal_ids),
        "selected_parameter_set_ids": list(application.selected_parameter_set_ids),
        "source_experiment_id": str(refined_spec.get("generator_id")),
        "source_artifact_id": f"{application.candidate_id}_{REFINEMENT_VERSION}",
        "source_artifact_path": str(original.prediction_artifact_path),
        "source_artifact_hash": source_artifact_hash,
        "signal_spec_version": SIGNAL_SPEC_VERSION,
        "prediction_schema_version": PREDICTION_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "as_of_timestamp": latest["as_of_timestamp"],
        "decision_timestamp": latest["decision_timestamp"],
        "target_asset": latest["target_asset"],
        "horizon": latest["horizon"],
        "signal_name": latest["signal_name"],
        "signal_value": latest["signal_value"],
        "signal_direction": latest["signal_direction"],
        "signal_confidence": latest["signal_confidence"],
        "refined_signal_value": latest["refined_signal_value"],
        "refined_signal_confidence": latest["refined_signal_confidence"],
        "valid_from": latest["valid_from"],
        "valid_until": latest["valid_until"],
        "input_snapshot_hash": input_snapshot_hash,
        "feature_snapshot_hash": feature_snapshot_hash,
        "model_or_rule_version": latest["model_or_rule_version"],
        "provenance": provenance,
        "prediction_records": list(refined_records),
        "source_schema_status": "candidate_bound",
        "record_count": len(refined_records),
        "generation_mode": MODE,
        "candidate_binding_method": "native_candidate_id",
        "historical_executable_artifact": True,
        "actual_path_validation_ready": False,
        "actual_path_validation_blocker": NEXT_TASK,
        "dynamic_promotion_status": "BLOCKED",
        "owner_review_required": False,
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
        "regeneration_executed": True,
        "actual_path_validation_executed": False,
        **candidate_artifact_safety_fields(),
        **trading_2281_boundary_fields(),
        "trading_2285_inconclusive_decisions_changed": False,
    }
    return clean_for_yaml(artifact)


def validate_refined_candidate_artifacts(
    *,
    refined_signal_spec: Mapping[str, Any],
    refined_signal_records: Sequence[Mapping[str, Any]],
    refined_prediction_artifact: Mapping[str, Any],
    application: ParameterApplication,
) -> dict[str, Any]:
    validator = CandidateSignalBindingValidator()
    spec_validation = validator.validate_candidate_signal_spec(refined_signal_spec)
    series_validation = validator.validate_candidate_bound_signal_series(refined_signal_records)
    artifact_validation = validator.validate_candidate_bound_prediction_artifact(
        refined_prediction_artifact
    )
    errors = (
        list(spec_validation.errors)
        + list(series_validation.errors)
        + list(artifact_validation.errors)
        + _refined_integrity_errors(
            refined_signal_spec=refined_signal_spec,
            refined_signal_records=refined_signal_records,
            refined_prediction_artifact=refined_prediction_artifact,
            application=application,
        )
    )
    return {
        "schema_version": "refined_candidate_generation_validation_summary.v1",
        "task_id": TASK_ID,
        "status": "PASS" if not errors else "FAIL",
        "candidate_id": application.candidate_id,
        "refined_candidate_id": application.refined_candidate_id,
        "mode": MODE,
        "signal_spec_validation": spec_validation.to_dict(),
        "signal_series_validation": series_validation.to_dict(),
        "prediction_artifact_validation": artifact_validation.to_dict(),
        "checked_signal_record_count": len(refined_signal_records),
        "checked_prediction_record_count": len(
            refined_prediction_artifact.get("prediction_records", [])
        )
        if isinstance(refined_prediction_artifact.get("prediction_records"), list)
        else 0,
        "candidate_bound_validator_reused": True,
        "refined_candidate_fail_closed_checks_satisfied": not errors,
        "errors": errors,
        **generator_operation_safety_fields(),
        "actual_path_validation_ready": False,
        "actual_path_validation_executed": False,
        "permanently_inconclusive_override_allowed": False,
        **trading_2281_boundary_fields(),
        "trading_2285_inconclusive_decisions_changed": False,
    }


def build_refined_parameter_application_report(
    application: ParameterApplication,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": "refined_parameter_application_report.v1",
            "task_id": TASK_ID,
            "candidate_id": application.candidate_id,
            "refined_candidate_id": application.refined_candidate_id,
            "selected_proposal_ids": list(application.selected_proposal_ids),
            "selected_parameter_set_ids": list(application.selected_parameter_set_ids),
            "selection_reason": (
                "Use TRADING-2287 implementation plan selected balanced parameter sets "
                "for confidence-scaling-only refined regeneration."
            ),
            "rejected_parameter_set_ids": list(application.rejected_parameter_set_ids),
            "rejection_reasons": dict(application.rejection_reasons),
            "applied_neutral_band_width": application.applied_neutral_band_width,
            "applied_confidence_scale_factor": application.applied_confidence_scale_factor,
            "applied_confidence_cap": application.applied_confidence_cap,
            "applied_confidence_floor": application.applied_confidence_floor,
            "applied_high_confidence_threshold": application.applied_high_confidence_threshold,
            "applied_directional_activation_threshold": (
                application.applied_directional_activation_threshold
            ),
            "applied_missing_input_penalty": application.applied_missing_input_penalty,
            "guardrail_profile": application.guardrail_profile,
            "guardrail_compliant": application.guardrail_compliant,
            **_operation_boundary_fields(),
        }
    )


def build_refined_original_vs_refined_delta(
    *,
    original_candidate_id: str,
    refined_candidate_id: str,
    original_records: Sequence[Mapping[str, Any]],
    refined_records: Sequence[Mapping[str, Any]],
    application: ParameterApplication,
) -> dict[str, Any]:
    original_stats = _distribution_stats(
        original_records,
        confidence_field="signal_confidence",
        value_field="signal_value",
        direction_field="signal_direction",
        high_threshold=application.applied_high_confidence_threshold,
        low_threshold=application.applied_low_confidence_threshold,
        activation_threshold=application.applied_directional_activation_threshold,
    )
    refined_stats = _distribution_stats(
        refined_records,
        confidence_field="refined_signal_confidence",
        value_field="refined_signal_value",
        direction_field="signal_direction",
        high_threshold=application.applied_high_confidence_threshold,
        low_threshold=application.applied_low_confidence_threshold,
        activation_threshold=application.applied_directional_activation_threshold,
    )
    direction_changed_count = sum(
        1 for row in refined_records if _bool(row.get("direction_changed"))
    )
    high_conviction_count = sum(
        1 for row in refined_records if _bool(row.get("high_conviction_flag"))
    )
    confidence_deltas = [
        to_float(row.get("confidence_delta")) for row in refined_records
    ]
    return clean_for_yaml(
        {
            "schema_version": "refined_original_vs_refined_delta.v1",
            "task_id": TASK_ID,
            "original_candidate_id": original_candidate_id,
            "refined_candidate_id": refined_candidate_id,
            "record_count_original": original_stats["record_count"],
            "record_count_refined": refined_stats["record_count"],
            "neutral_ratio_original": original_stats["neutral_ratio"],
            "neutral_ratio_refined": refined_stats["neutral_ratio"],
            "directional_signal_ratio_original": original_stats["directional_signal_ratio"],
            "directional_signal_ratio_refined": refined_stats["directional_signal_ratio"],
            "high_confidence_ratio_original": original_stats["high_confidence_ratio"],
            "high_confidence_ratio_refined": refined_stats["high_confidence_ratio"],
            "low_confidence_ratio_original": original_stats["low_confidence_ratio"],
            "low_confidence_ratio_refined": refined_stats["low_confidence_ratio"],
            "median_confidence_original": original_stats["median_confidence"],
            "median_confidence_refined": refined_stats["median_confidence"],
            "p75_confidence_original": original_stats["p75_confidence"],
            "p75_confidence_refined": refined_stats["p75_confidence"],
            "p90_confidence_original": original_stats["p90_confidence"],
            "p90_confidence_refined": refined_stats["p90_confidence"],
            "confidence_std_original": original_stats["confidence_std"],
            "confidence_std_refined": refined_stats["confidence_std"],
            "risk_on_count_original": original_stats["direction_counts"].get("risk_on", 0),
            "risk_on_count_refined": refined_stats["direction_counts"].get("risk_on", 0),
            "risk_off_count_original": original_stats["direction_counts"].get("risk_off", 0),
            "risk_off_count_refined": refined_stats["direction_counts"].get("risk_off", 0),
            "trend_confirming_count_original": original_stats["direction_counts"].get(
                "trend_confirming",
                0,
            ),
            "trend_confirming_count_refined": refined_stats["direction_counts"].get(
                "trend_confirming",
                0,
            ),
            "trend_weakening_count_original": original_stats["direction_counts"].get(
                "trend_weakening",
                0,
            ),
            "trend_weakening_count_refined": refined_stats["direction_counts"].get(
                "trend_weakening",
                0,
            ),
            "volatility_expansion_count_original": original_stats["direction_counts"].get(
                "volatility_expansion",
                0,
            ),
            "volatility_expansion_count_refined": refined_stats["direction_counts"].get(
                "volatility_expansion",
                0,
            ),
            "volatility_compression_count_original": original_stats[
                "direction_counts"
            ].get("volatility_compression", 0),
            "volatility_compression_count_refined": refined_stats[
                "direction_counts"
            ].get("volatility_compression", 0),
            "direction_changed_count": direction_changed_count,
            "high_conviction_flag_count": high_conviction_count,
            "average_confidence_delta": round_float(_mean(confidence_deltas)),
            "guardrail_compliant": application.guardrail_compliant,
            "actual_path_validation_executed": False,
            **_operation_boundary_fields(),
        }
    )


def build_refined_generation_summary(
    *,
    original: OriginalCandidateArtifacts,
    refined_signal_records: Sequence[Mapping[str, Any]],
    refined_prediction_artifact: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    parameter_report: Mapping[str, Any],
    delta: Mapping[str, Any],
    generated_at: datetime,
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": "refined_candidate_generation_summary.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "summary": {
                "task_id": TASK_ID,
                "original_candidate_id": original.candidate_id,
                "refined_candidate_id": refined_prediction_artifact.get("candidate_id"),
                "candidate_family": refined_prediction_artifact.get("candidate_family"),
                "artifact_role": REFINED_ARTIFACT_ROLE,
                "refinement_source_task": REFINEMENT_SOURCE_TASK,
                "refinement_version": REFINEMENT_VERSION,
                "mode": MODE,
                "signal_record_count": len(refined_signal_records),
                "prediction_record_count": len(
                    refined_prediction_artifact.get("prediction_records", [])
                ),
                "validation_status": validation_summary.get("status"),
                "selected_proposal_ids": parameter_report.get("selected_proposal_ids"),
                "selected_parameter_set_ids": parameter_report.get(
                    "selected_parameter_set_ids"
                ),
                "high_conviction_flag_count": delta.get("high_conviction_flag_count"),
                "actual_path_validation_ready": False,
                "actual_path_validation_executed": False,
                "next_task": NEXT_TASK,
                **candidate_artifact_safety_fields(),
                **_operation_boundary_fields(),
            },
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "parameter_application_report": dict(parameter_report),
            "original_vs_refined_delta": dict(delta),
            "validation_summary": dict(validation_summary),
            "historical_executable_artifact": True,
            "actual_path_validation_ready": False,
            "dynamic_promotion_status": "BLOCKED",
            **candidate_artifact_safety_fields(),
            **_operation_boundary_fields(),
        }
    )


def build_refined_top_level_validation_summary(
    *,
    candidate_results: Sequence[Mapping[str, Any]],
    validation_summaries: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    errors = [
        str(error)
        for validation in validation_summaries
        for error in validation.get("errors", [])
    ]
    return {
        "schema_version": "refined_candidate_regeneration_validation_summary.v1",
        "task_id": TASK_ID,
        "status": "PASS" if not errors else "FAIL",
        "generated_at": generated_at.isoformat(),
        "candidate_count": len(candidate_results),
        "original_candidate_ids": [
            str(item.get("original_candidate_id")) for item in candidate_results
        ],
        "refined_candidate_ids": [
            str(item.get("refined_candidate_id")) for item in candidate_results
        ],
        "candidate_validation_statuses": {
            str(item.get("refined_candidate_id")): item.get(
                "validation_summary",
                {},
            ).get("status")
            for item in candidate_results
        },
        "candidate_bound_validator_reused": True,
        "errors": errors,
        **_operation_boundary_fields(),
        "actual_path_validation_ready": False,
        "actual_path_validation_executed": False,
        "permanently_inconclusive_override_allowed": False,
    }


def build_refined_regeneration_run_summary(
    *,
    inputs: RefinedCandidateInputs,
    candidate_ids: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    selected_scaling_proposal_count: int,
    selected_parameter_set_count: int,
    candidate_results: Sequence[Mapping[str, Any]],
    validation_summary: Mapping[str, Any],
    generated_at: datetime,
    mode: str,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": "refined_candidate_regeneration_run_summary.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": mode,
            "input_actual_path_records_from_2287": inputs.summary.get(
                "input_prediction_outcome_record_count"
            ),
            "input_eligible_records_from_2287": inputs.summary.get(
                "input_eligible_record_count"
            ),
            "candidate_count": len(candidate_ids),
            "original_candidate_ids": list(candidate_ids),
            "refined_candidate_ids": [
                str(item.get("refined_candidate_id")) for item in candidate_results
            ],
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "selected_scaling_proposal_count": selected_scaling_proposal_count,
            "selected_parameter_set_count": selected_parameter_set_count,
            "validation_status": validation_summary.get("status"),
            "regeneration_executed": True,
            "actual_path_validation_executed": False,
            "actual_path_validation_ready": False,
            "historical_executable_artifact": True,
            "next_task": NEXT_TASK,
            **candidate_artifact_safety_fields(),
            **_operation_boundary_fields(),
        }
    )


def build_refined_original_vs_refined_delta_summary(
    *,
    delta_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": "refined_original_vs_refined_delta_summary.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "candidate_count": len(delta_rows),
            "rows": list(delta_rows),
            "average_high_confidence_ratio_original": round_float(
                _mean([to_float(row.get("high_confidence_ratio_original")) for row in delta_rows])
            ),
            "average_high_confidence_ratio_refined": round_float(
                _mean([to_float(row.get("high_confidence_ratio_refined")) for row in delta_rows])
            ),
            "average_directional_signal_ratio_original": round_float(
                _mean(
                    [
                        to_float(row.get("directional_signal_ratio_original"))
                        for row in delta_rows
                    ]
                )
            ),
            "average_directional_signal_ratio_refined": round_float(
                _mean(
                    [
                        to_float(row.get("directional_signal_ratio_refined"))
                        for row in delta_rows
                    ]
                )
            ),
            "actual_path_validation_executed": False,
            **_operation_boundary_fields(),
        }
    )


def write_refined_signal_series_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    required = list(candidate_bound_signal_series_contract_dict()["required_columns"])
    extra_fields = [
        "original_candidate_id",
        "refined_candidate_id",
        "refinement_source_task",
        "refinement_task_id",
        "refinement_version",
        "confidence_scaling_method",
        "high_conviction_rule_id",
        "high_conviction_flag",
        "original_signal_value",
        "original_signal_confidence",
        "refined_signal_value",
        "refined_signal_confidence",
        "confidence_delta",
        "direction_changed",
        "neutral_band_width",
        "directional_activation_threshold",
        "confidence_cap",
        "confidence_floor",
        "guardrail_profile",
        "selected_proposal_ids",
        "selected_parameter_set_ids",
        "actual_path_validation_ready",
        "actual_path_validation_executed",
    ]
    discovered = []
    seen = set(required)
    for field in extra_fields:
        if field not in seen:
            discovered.append(field)
            seen.add(field)
    for row in rows:
        for field in row:
            if field not in seen and field != "prediction_fields":
                discovered.append(field)
                seen.add(field)
    fieldnames = required + discovered
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: _csv_value(row.get(field))
                    for field in fieldnames
                }
            )


def _load_original_candidate_artifacts(
    *,
    original_generator_dir: Path,
    candidate_id: str,
) -> OriginalCandidateArtifacts:
    if candidate_id not in REFINED_CANDIDATE_IDS:
        raise RefinedCandidateRegenerationError(f"unknown refined candidate: {candidate_id}")
    candidate_dir = original_generator_dir / candidate_id
    paths = {
        name: candidate_dir / filename
        for name, filename in REQUIRED_ORIGINAL_CANDIDATE_FILES.items()
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} missing original generator artifact(s): {missing}"
        )
    signal_spec = _read_json(paths["signal_spec"])
    prediction_artifact = _read_json(paths["prediction_artifact"])
    generation_summary = _read_json(paths["generation_summary"])
    validation_summary = _read_json(paths["validation_summary"])
    signal_series_rows = _read_signal_series_csv(paths["signal_series"])
    prediction_records = _records(prediction_artifact.get("prediction_records"))
    if not prediction_records:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} original prediction artifact has no prediction_records"
        )

    _assert_original_artifact_safe(candidate_id, signal_spec, prediction_artifact)
    _assert_original_artifact_safe(candidate_id, generation_summary, validation_summary)
    validator = CandidateSignalBindingValidator()
    spec_validation = validator.validate_candidate_signal_spec(signal_spec)
    series_validation = validator.validate_candidate_bound_signal_series(signal_series_rows)
    artifact_validation = validator.validate_candidate_bound_prediction_artifact(
        prediction_artifact
    )
    errors = (
        list(spec_validation.errors)
        + list(series_validation.errors)
        + list(artifact_validation.errors)
        + _original_record_integrity_errors(candidate_id, prediction_records)
    )
    if errors:
        raise RefinedCandidateRegenerationError(
            f"{candidate_id} original artifact validation failed: {errors}"
        )
    return OriginalCandidateArtifacts(
        candidate_id=candidate_id,
        refined_candidate_id=_refined_candidate_id(candidate_id),
        candidate_dir=candidate_dir,
        signal_spec_path=paths["signal_spec"],
        signal_series_path=paths["signal_series"],
        prediction_artifact_path=paths["prediction_artifact"],
        generation_summary_path=paths["generation_summary"],
        validation_summary_path=paths["validation_summary"],
        signal_spec=signal_spec,
        signal_series_rows=signal_series_rows,
        prediction_artifact=prediction_artifact,
        prediction_records=prediction_records,
        generation_summary=generation_summary,
        validation_summary=validation_summary,
    )


def _refined_record_payload(
    *,
    row: Mapping[str, Any],
    original: OriginalCandidateArtifacts,
    application: ParameterApplication,
    provenance_source_paths: Sequence[str],
    provenance_source_hashes: Sequence[str],
    generated_at: datetime,
    source_artifact_hash: str,
    input_snapshot_hash: str,
    feature_snapshot_hash: str,
    refined_signal_value: float,
    refined_signal_direction: str,
    refined_signal_confidence: float,
    direction_changed: bool,
) -> dict[str, Any]:
    original_value = to_float(row.get("signal_value"))
    original_confidence = to_float(row.get("signal_confidence"))
    provenance = _refined_provenance(
        original=original,
        row=row,
        application=application,
        provenance_source_paths=provenance_source_paths,
        provenance_source_hashes=provenance_source_hashes,
    )
    payload = dict(row)
    payload.update(
        {
            "candidate_id": application.refined_candidate_id,
            "original_candidate_id": application.candidate_id,
            "refined_candidate_id": application.refined_candidate_id,
            "source_experiment_id": f"{application.refined_candidate_id}_generator",
            "source_artifact_id": f"{application.candidate_id}_{REFINEMENT_VERSION}",
            "source_artifact_path": str(original.signal_series_path),
            "source_artifact_hash": source_artifact_hash,
            "signal_spec_version": SIGNAL_SPEC_VERSION,
            "prediction_schema_version": PREDICTION_SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "signal_value": refined_signal_value,
            "signal_direction": refined_signal_direction,
            "signal_confidence": refined_signal_confidence,
            "input_snapshot_hash": input_snapshot_hash,
            "feature_snapshot_hash": feature_snapshot_hash,
            "model_or_rule_version": (
                f"{row.get('model_or_rule_version')}.{REFINEMENT_VERSION}"
            ),
            "provenance": provenance,
            "refinement_source_task": REFINEMENT_SOURCE_TASK,
            "refinement_task_id": TASK_ID,
            "refinement_version": REFINEMENT_VERSION,
            "confidence_scaling_method": "guardrail_bounded_piecewise_directional_scaling",
            "high_conviction_rule_id": "guardrail_bounded_high_conviction_v1",
            "high_conviction_flag": False,
            "original_signal_value": round_float(original_value),
            "original_signal_confidence": round_float(original_confidence),
            "refined_signal_value": refined_signal_value,
            "refined_signal_confidence": refined_signal_confidence,
            "confidence_delta": round_float(refined_signal_confidence - original_confidence),
            "direction_changed": direction_changed,
            "neutral_band_width": application.applied_neutral_band_width,
            "directional_activation_threshold": (
                application.applied_directional_activation_threshold
            ),
            "confidence_cap": application.applied_confidence_cap,
            "confidence_floor": application.applied_confidence_floor,
            "guardrail_profile": application.guardrail_profile,
            "selected_proposal_ids": list(application.selected_proposal_ids),
            "selected_parameter_set_ids": list(application.selected_parameter_set_ids),
            "actual_path_validation_ready": False,
            "actual_path_validation_executed": False,
            **candidate_artifact_safety_fields(),
            **trading_2281_boundary_fields(),
            "trading_2285_inconclusive_decisions_changed": False,
        }
    )
    return payload


def _refined_provenance(
    *,
    original: OriginalCandidateArtifacts,
    row: Mapping[str, Any],
    application: ParameterApplication,
    provenance_source_paths: Sequence[str],
    provenance_source_hashes: Sequence[str],
) -> dict[str, Any]:
    original_provenance = _provenance(row)
    return clean_for_yaml(
        {
            **original_provenance,
            "source_paths": list(provenance_source_paths),
            "source_hashes": list(provenance_source_hashes),
            "regeneration_mode": "deterministic_refined_regeneration",
            "pit_policy": original_provenance.get("pit_policy", "strict_pit"),
            "candidate_binding_method": "native_candidate_id",
            "source_schema_status": "candidate_bound",
            "refinement_source": {
                "task_id": REFINEMENT_SOURCE_TASK,
                "refinement_task_id": TASK_ID,
                "refinement_version": REFINEMENT_VERSION,
                "proposal_ids": list(application.selected_proposal_ids),
                "parameter_set_ids": list(application.selected_parameter_set_ids),
            },
            "promotion_eligible": False,
        }
    )


def _scaled_confidence(
    *,
    original_signal_value: float,
    original_signal_confidence: float,
    refined_signal_direction: str,
    provenance: Mapping[str, Any],
    application: ParameterApplication,
) -> float:
    directional_strength = 0.0
    activation = application.applied_directional_activation_threshold
    if abs(original_signal_value) > activation and activation < 1.0:
        directional_strength = (abs(original_signal_value) - activation) / (1.0 - activation)
    scaled = original_signal_confidence * (
        1.0
        + (application.applied_confidence_scale_factor - 1.0)
        * max(0.0, min(1.0, directional_strength))
    )
    missing_inputs = _strings(provenance.get("missing_inputs"))
    if missing_inputs:
        scaled -= application.applied_missing_input_penalty
    if refined_signal_direction == "neutral":
        scaled = min(
            scaled,
            application.applied_high_confidence_threshold - NEUTRAL_HIGH_CONFIDENCE_MARGIN,
        )
    bounded = max(
        application.applied_confidence_floor,
        min(application.applied_confidence_cap, scaled),
    )
    return round_float(max(0.0, min(1.0, bounded)))


def _refined_direction(
    *,
    candidate_id: str,
    signal_name: str,
    signal_value: float,
    neutral_band_width: float,
) -> str:
    if abs(signal_value) < neutral_band_width:
        return "neutral"
    if candidate_id == "baseline_plus_trend_structure":
        if signal_name == "relative_strength_score":
            return "risk_on" if signal_value > 0 else "risk_off"
        return "trend_confirming" if signal_value > 0 else "trend_weakening"
    if candidate_id == "risk_appetite":
        return "risk_on" if signal_value > 0 else "risk_off"
    if candidate_id == "volatility_regime":
        if signal_name == "stress_regime_score":
            return "risk_off" if signal_value < 0 else "neutral"
        return "volatility_compression" if signal_value > 0 else "volatility_expansion"
    return str(signal_value)


def _high_conviction_base_condition(
    row: Mapping[str, Any],
    application: ParameterApplication,
) -> bool:
    return (
        to_float(row.get("refined_signal_confidence"))
        >= application.applied_high_confidence_threshold
        and abs(to_float(row.get("refined_signal_value")))
        >= application.applied_directional_activation_threshold
        and str(row.get("signal_direction") or "") != "neutral"
    )


def _distribution_stats(
    rows: Sequence[Mapping[str, Any]],
    *,
    confidence_field: str,
    value_field: str,
    direction_field: str,
    high_threshold: float,
    low_threshold: float,
    activation_threshold: float,
) -> dict[str, Any]:
    record_count = len(rows)
    confidences = [to_float(row.get(confidence_field)) for row in rows]
    directions = [str(row.get(direction_field) or "") for row in rows]
    values = [to_float(row.get(value_field)) for row in rows]
    neutral_count = sum(1 for direction in directions if direction == "neutral")
    high_count = sum(
        1
        for direction, confidence, value in zip(directions, confidences, values, strict=False)
        if confidence >= high_threshold
        and abs(value) >= activation_threshold
        and direction != "neutral"
    )
    low_count = sum(1 for confidence in confidences if confidence <= low_threshold)
    direction_counts = Counter(directions)
    return {
        "record_count": record_count,
        "neutral_ratio": round_float(_ratio(neutral_count, record_count)),
        "directional_signal_ratio": round_float(_ratio(record_count - neutral_count, record_count)),
        "high_confidence_ratio": round_float(_ratio(high_count, record_count)),
        "low_confidence_ratio": round_float(_ratio(low_count, record_count)),
        "median_confidence": round_float(_percentile(confidences, 0.5)),
        "p75_confidence": round_float(_percentile(confidences, 0.75)),
        "p90_confidence": round_float(_percentile(confidences, 0.9)),
        "confidence_std": round_float(_std(confidences)),
        "direction_counts": dict(direction_counts),
    }


def _refined_integrity_errors(
    *,
    refined_signal_spec: Mapping[str, Any],
    refined_signal_records: Sequence[Mapping[str, Any]],
    refined_prediction_artifact: Mapping[str, Any],
    application: ParameterApplication,
) -> list[str]:
    errors: list[str] = []
    if application.refined_candidate_id == application.candidate_id:
        errors.append("refined_candidate_id equals original_candidate_id")
    for scope, payload in (
        ("refined_signal_spec", refined_signal_spec),
        ("refined_prediction_artifact", refined_prediction_artifact),
    ):
        _check_refined_payload(scope, payload, application, errors)
    if refined_prediction_artifact.get("artifact_role") != REFINED_ARTIFACT_ROLE:
        errors.append("refined_prediction_artifact: invalid artifact_role")
    if refined_prediction_artifact.get("historical_executable_artifact") is not True:
        errors.append("refined_prediction_artifact: historical_executable_artifact must be true")
    if refined_prediction_artifact.get("actual_path_validation_ready") is not False:
        errors.append("refined_prediction_artifact: actual_path_validation_ready must be false")
    if refined_prediction_artifact.get("actual_path_validation_executed") is not False:
        errors.append("refined_prediction_artifact: actual_path_validation_executed must be false")
    if not refined_prediction_artifact.get("selected_parameter_set_ids"):
        errors.append("refined_prediction_artifact: missing selected_parameter_set_ids")
    high_count = 0
    for index, row in enumerate(refined_signal_records):
        scope = f"refined_signal_series[{index}]"
        _check_refined_payload(scope, row, application, errors)
        if _bool(row.get("high_conviction_flag")):
            high_count += 1
        for field in ("refined_signal_value", "refined_signal_confidence"):
            value = to_float(row.get(field), default=float("nan"))
            if not math.isfinite(value):
                errors.append(f"{scope}: {field} must be finite")
        if to_float(row.get("refined_signal_confidence")) < 0.0 or to_float(
            row.get("refined_signal_confidence")
        ) > 1.0:
            errors.append(f"{scope}: refined_signal_confidence out of [0, 1]")
    max_allowed = math.floor(len(refined_signal_records) * application.max_high_confidence_ratio)
    if high_count > max_allowed:
        errors.append("refined_signal_series: high_confidence_ratio exceeds guardrail")
    return errors


def _check_refined_payload(
    scope: str,
    payload: Mapping[str, Any],
    application: ParameterApplication,
    errors: list[str],
) -> None:
    for field in (
        "original_candidate_id",
        "refined_candidate_id",
        "refinement_source_task",
        "refinement_task_id",
        "refinement_version",
    ):
        if _is_missing(payload.get(field)):
            errors.append(f"{scope}: missing {field}")
    if payload.get("original_candidate_id") != application.candidate_id:
        errors.append(f"{scope}: original_candidate_id mismatch")
    if payload.get("refined_candidate_id") != application.refined_candidate_id:
        errors.append(f"{scope}: refined_candidate_id mismatch")
    if payload.get("candidate_id") != application.refined_candidate_id:
        errors.append(f"{scope}: candidate_id must be refined candidate id")
    if payload.get("refinement_source_task") != REFINEMENT_SOURCE_TASK:
        errors.append(f"{scope}: refinement_source_task must be {REFINEMENT_SOURCE_TASK}")
    if payload.get("refinement_task_id") != TASK_ID:
        errors.append(f"{scope}: refinement_task_id must be {TASK_ID}")
    if payload.get("refinement_version") != REFINEMENT_VERSION:
        errors.append(f"{scope}: refinement_version must be {REFINEMENT_VERSION}")
    if _bool(payload.get("promotion_eligible")) is not False:
        errors.append(f"{scope}: promotion_eligible must be false")
    for field in ("promotion_allowed", "paper_shadow_allowed", "production_allowed"):
        if _bool(payload.get(field)) is not False:
            errors.append(f"{scope}: {field} must be false")
    if str(payload.get("broker_action") or "") != "none":
        errors.append(f"{scope}: broker_action must be none")
    provenance = _provenance(payload)
    if provenance:
        if provenance.get("regeneration_mode") != "deterministic_refined_regeneration":
            errors.append(
                f"{scope}: provenance.regeneration_mode must be deterministic_refined_regeneration"
            )
        refinement_source = _mapping(provenance.get("refinement_source"))
        if refinement_source.get("task_id") != REFINEMENT_SOURCE_TASK:
            errors.append(f"{scope}: missing provenance.refinement_source.task_id")
        if not refinement_source.get("parameter_set_ids"):
            errors.append(f"{scope}: missing provenance.refinement_source.parameter_set_ids")


def _original_record_integrity_errors(
    candidate_id: str,
    records: Sequence[Mapping[str, Any]],
) -> list[str]:
    errors: list[str] = []
    required = (
        "candidate_id",
        "source_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "horizon",
        "provenance",
    )
    for index, record in enumerate(records):
        scope = f"{candidate_id}.prediction_records[{index}]"
        for field in required:
            if _is_missing(record.get(field)):
                errors.append(f"{scope}: missing {field}")
        if record.get("candidate_id") != candidate_id:
            errors.append(f"{scope}: candidate_id mismatch")
        for field in ("promotion_allowed", "paper_shadow_allowed", "production_allowed"):
            if _bool(record.get(field)) is not False:
                errors.append(f"{scope}: {field} must be false")
        if str(record.get("broker_action") or "") != "none":
            errors.append(f"{scope}: broker_action must be none")
    return errors


def _assert_original_artifact_safe(
    candidate_id: str,
    *payloads: Mapping[str, Any],
) -> None:
    for payload in payloads:
        _assert_input_payload_safe(candidate_id, payload)


def _assert_input_payload_safe(scope: str, payload: Any) -> None:
    for path, item in _walk_payload(payload):
        label = f"{scope}{path}"
        if isinstance(item, Mapping):
            for field in ("promotion_allowed", "paper_shadow_allowed", "production_allowed"):
                if field in item and _bool(item.get(field)) is not False:
                    raise RefinedCandidateRegenerationError(f"{label}: {field} must be false")
            if "broker_action" in item and str(item.get("broker_action") or "") != "none":
                raise RefinedCandidateRegenerationError(f"{label}: broker_action must be none")


def _assert_generated_payload_safe(scope: str, payload: Any) -> None:
    _assert_input_payload_safe(scope, payload)
    _assert_no_banned_recommendations(scope, payload)


def _assert_no_banned_recommendations(scope: str, payload: Any) -> None:
    for path, item in _walk_payload(payload):
        if isinstance(item, str) and item in BANNED_RECOMMENDATIONS:
            raise RefinedCandidateRegenerationError(
                f"{scope}{path}: banned readiness recommendation {item}"
            )


def _write_docs(
    *,
    docs_root: Path,
    run_summary: Mapping[str, Any],
    candidate_results: Sequence[Mapping[str, Any]],
    delta_rows: Sequence[Mapping[str, Any]],
) -> None:
    write_markdown(
        docs_root / "refined_candidate_regeneration_report.md",
        _render_regeneration_report(run_summary=run_summary, delta_rows=delta_rows),
    )
    write_markdown(
        docs_root / "refined_candidate_parameter_application_report.md",
        _render_parameter_application_report(candidate_results),
    )
    write_markdown(
        docs_root / "refined_original_vs_refined_delta_summary.md",
        _render_delta_summary(delta_rows),
    )


def _render_regeneration_report(
    *,
    run_summary: Mapping[str, Any],
    delta_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Refined Candidate Regeneration Report",
        "",
        f"- status: `{run_summary.get('status')}`",
        f"- task_id: `{TASK_ID}`",
        "- TRADING-2287 primary diagnosis: `INSUFFICIENT_HIGH_CONVICTION_RULE`",
        (
            "- TRADING-2288 只执行 refined regeneration；actual-path validation "
            "阻断到 TRADING-2289。"
        ),
        "- promotion_allowed: `false`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "",
        "|original_candidate_id|refined_candidate_id|neutral_ratio_original|neutral_ratio_refined|directional_ratio_original|directional_ratio_refined|high_confidence_ratio_original|high_confidence_ratio_refined|",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in delta_rows:
        lines.append(
            "|{original}|{refined}|{n0}|{n1}|{d0}|{d1}|{h0}|{h1}|".format(
                original=row.get("original_candidate_id"),
                refined=row.get("refined_candidate_id"),
                n0=row.get("neutral_ratio_original"),
                n1=row.get("neutral_ratio_refined"),
                d0=row.get("directional_signal_ratio_original"),
                d1=row.get("directional_signal_ratio_refined"),
                h0=row.get("high_confidence_ratio_original"),
                h1=row.get("high_confidence_ratio_refined"),
            )
        )
    lines.extend(
        [
            "",
            (
                "TRADING-2289 必须验证 high-confidence alignment 是否改善、false "
                "risk-on / false risk-off 成本是否恶化，以及 refined confidence "
                "scaling 是否只是放大噪音。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_parameter_application_report(
    candidate_results: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Refined Candidate Parameter Application Report",
        "",
        (
            "每行使用 TRADING-2287 selected proposals 和 parameter sets。"
            "这不是 owner review approval。"
        ),
        "",
        "|candidate_id|refined_candidate_id|selected_proposal_ids|selected_parameter_set_ids|guardrail_compliant|",
        "|---|---|---|---|---|",
    ]
    for result in candidate_results:
        report = _mapping(result.get("parameter_application_report"))
        lines.append(
            "|{candidate}|{refined}|{proposals}|{sets}|{guardrail}|".format(
                candidate=report.get("candidate_id"),
                refined=report.get("refined_candidate_id"),
                proposals=", ".join(_strings(report.get("selected_proposal_ids"))),
                sets=", ".join(_strings(report.get("selected_parameter_set_ids"))),
                guardrail=report.get("guardrail_compliant"),
            )
        )
    return "\n".join(lines) + "\n"


def _render_delta_summary(delta_rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Refined Original vs Refined Delta Summary",
        "",
        (
            "本报告只比较 signal distribution，不比较 future outcomes、utility、"
            "owner review readiness、paper-shadow readiness、production readiness "
            "或 broker action readiness。"
        ),
        "",
        "|original_candidate_id|refined_candidate_id|median_confidence_original|median_confidence_refined|p90_confidence_original|p90_confidence_refined|direction_changed_count|high_conviction_flag_count|",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in delta_rows:
        lines.append(
            "|{original}|{refined}|{m0}|{m1}|{p0}|{p1}|{changed}|{high}|".format(
                original=row.get("original_candidate_id"),
                refined=row.get("refined_candidate_id"),
                m0=row.get("median_confidence_original"),
                m1=row.get("median_confidence_refined"),
                p0=row.get("p90_confidence_original"),
                p1=row.get("p90_confidence_refined"),
                changed=row.get("direction_changed_count"),
                high=row.get("high_conviction_flag_count"),
            )
        )
    return "\n".join(lines) + "\n"


def _candidate_output_paths(candidate_dir: Path) -> dict[str, Path]:
    return {
        "refined_candidate_signal_spec_json": candidate_dir
        / "refined_candidate_signal_spec.json",
        "refined_candidate_signal_series_csv": candidate_dir
        / "refined_candidate_signal_series.csv",
        "refined_candidate_prediction_artifact_json": candidate_dir
        / "refined_candidate_prediction_artifact.json",
        "refined_generation_summary_json": candidate_dir / "refined_generation_summary.json",
        "refined_validation_summary_json": candidate_dir / "refined_validation_summary.json",
        "refined_parameter_application_report_json": candidate_dir
        / "refined_parameter_application_report.json",
        "refined_original_vs_refined_delta_json": candidate_dir
        / "refined_original_vs_refined_delta.json",
    }


def _refined_source_lineage(
    *,
    original: OriginalCandidateArtifacts,
    refinement_plan_dir: Path,
) -> tuple[list[str], list[str]]:
    refinement_summary_path = refinement_plan_dir / REQUIRED_REFINEMENT_PLAN_FILES["summary"]
    parameter_grid_path = refinement_plan_dir / REQUIRED_REFINEMENT_PLAN_FILES["parameter_grid"]
    source_paths = [
        original.signal_series_path,
        original.prediction_artifact_path,
        original.generation_summary_path,
        refinement_summary_path,
        parameter_grid_path,
    ]
    return [str(path) for path in source_paths], [_sha256(path) for path in source_paths]


def _filter_records(
    records: Sequence[Mapping[str, Any]],
    *,
    target_assets: Sequence[str],
    horizons: Sequence[str],
) -> list[dict[str, Any]]:
    asset_set = set(target_assets)
    horizon_set = set(horizons)
    return [
        dict(row)
        for row in records
        if str(row.get("target_asset")) in asset_set and str(row.get("horizon")) in horizon_set
    ]


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    parsed = tuple(str(item).strip() for item in value if str(item).strip())
    if not parsed:
        raise RefinedCandidateRegenerationError("input list must be non-empty")
    return parsed


def _refined_candidate_id(candidate_id: str) -> str:
    refined = REFINED_CANDIDATE_IDS.get(candidate_id)
    if not refined:
        raise RefinedCandidateRegenerationError(f"unknown candidate_id: {candidate_id}")
    return refined


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        raise RefinedCandidateRegenerationError(f"JSON must be object: {path}")
    return dict(payload)


def _read_signal_series_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            parsed = dict(row)
            for field in ("provenance", "source_prediction_flags"):
                if isinstance(parsed.get(field), str) and str(parsed[field]).strip():
                    try:
                        parsed[field] = json.loads(str(parsed[field]))
                    except json.JSONDecodeError:
                        pass
            rows.append(parsed)
    return rows


def _rows_from_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _single_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
    label: str,
) -> dict[str, Any]:
    matches = [dict(row) for row in rows if str(row.get("candidate_id")) == candidate_id]
    if len(matches) != 1:
        raise RefinedCandidateRegenerationError(
            f"{label} requires exactly one row for {candidate_id}, got {len(matches)}"
        )
    return matches[0]


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if isinstance(value, list | tuple | set):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _provenance(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = row.get("provenance")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _mean_field(rows: Sequence[Mapping[str, Any]], field: str) -> float:
    return round_float(_mean([to_float(row.get(field)) for row in rows]))


def _mean(values: Sequence[float]) -> float:
    clean = [value for value in values if math.isfinite(value)]
    return sum(clean) / len(clean) if clean else 0.0


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _percentile(values: Sequence[float], quantile: float) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _std(values: Sequence[float]) -> float:
    clean = [value for value in values if math.isfinite(value)]
    if len(clean) < 2:
        return 0.0
    mean = sum(clean) / len(clean)
    variance = sum((value - mean) ** 2 for value in clean) / len(clean)
    return math.sqrt(variance)


def _csv_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return json.dumps(clean_for_yaml(dict(value)), ensure_ascii=False, sort_keys=True)
    if isinstance(value, list | tuple):
        return json.dumps(clean_for_yaml(list(value)), ensure_ascii=False, sort_keys=True)
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(clean_for_yaml(dict(payload)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _operation_boundary_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "owner_review_required": False,
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
        **trading_2281_boundary_fields(),
        "trading_2285_inconclusive_decisions_changed": False,
    }


def _walk_payload(value: Any, path: str = ""):
    yield path, value
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield from _walk_payload(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_payload(item, f"{path}[{index}]")
