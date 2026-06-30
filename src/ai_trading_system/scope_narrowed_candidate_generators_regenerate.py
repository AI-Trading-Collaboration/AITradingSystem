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

from ai_trading_system.candidate_signal_binding_schema import (
    PREDICTION_SCHEMA_VERSION,
    SIGNAL_SPEC_VERSION,
    candidate_bound_signal_series_contract_dict,
)
from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
from ai_trading_system.config import PROJECT_ROOT
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
from ai_trading_system.refined_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REFINED_VALIDATION_ROOT,
)
from ai_trading_system.refined_candidate_generators_regenerate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REFINED_GENERATOR_ROOT,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_REVIEW_ROOT,
)
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "scope_narrowed_candidate_generators_regenerated"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2291_SCOPE_NARROWED_CANDIDATE_REGENERATION"
STATUS = "SCOPE_NARROWED_CANDIDATE_ARTIFACTS_READY_ACTUAL_PATH_VALIDATION_BLOCKED"
MODE = "scope_narrowed_regeneration"
SCOPE_SOURCE_TASK = "TRADING-2290"
SCOPE_NARROWING_VERSION = "scope_narrowed_v1"
SCOPE_FILTER_VERSION = "scope_filter_v1"
ARTIFACT_ROLE = "scope_narrowed_regenerated_candidate_artifact"
NEXT_TASK = "TRADING-2292_Scope_Narrowed_Candidate_Actual_Path_Validation"
REGENERATION_MODE = "deterministic_scope_narrowed_regeneration"

DEFAULT_INCLUDE_CANDIDATES = (
    "baseline_plus_trend_structure_refined_confidence_v1",
    "volatility_regime_refined_confidence_v1",
)
DEFAULT_ARCHIVE_CANDIDATES = ("risk_appetite_refined_confidence_v1",)
DEFAULT_TARGET_ASSETS = ("QQQ", "SPY", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")

SCOPE_NARROWED_CANDIDATE_MAP = {
    "baseline_plus_trend_structure_refined_confidence_v1": {
        "original_candidate_id": "baseline_plus_trend_structure",
        "scope_narrowed_candidate_id": (
            "baseline_plus_trend_structure_scope_narrowed_confirmation_v1"
        ),
        "usage_role": "confirmation_only",
    },
    "volatility_regime_refined_confidence_v1": {
        "original_candidate_id": "volatility_regime",
        "scope_narrowed_candidate_id": "volatility_regime_scope_narrowed_risk_cap_v1",
        "usage_role": "risk_cap_only",
    },
}
RISK_APPETITE_ARCHIVE_CANDIDATE = "risk_appetite_refined_confidence_v1"

REQUIRED_SCOPE_REVIEW_FILES = {
    "summary": "local_edge_scope_review_summary.json",
    "scope_recommendation": "candidate_scope_narrowing_recommendation_matrix.json",
    "direction_scope": "candidate_direction_scope_matrix.json",
    "high_conviction_scope": "candidate_high_conviction_scope_matrix.json",
    "false_cost_scope": "candidate_false_cost_scope_matrix.json",
    "risk_appetite_reject": "risk_appetite_reject_record.json",
    "next_task": "candidate_next_task_recommendation_matrix.json",
}
OPTIONAL_SCOPE_REVIEW_FILES = {
    "asset_scope": "candidate_asset_scope_matrix.json",
    "horizon_scope": "candidate_horizon_scope_matrix.json",
    "regime_scope": "candidate_regime_scope_matrix.json",
    "decision_summary": "candidate_scope_review_decision_summary.json",
}
REQUIRED_REFINED_GENERATOR_TOP_LEVEL_FILES = {
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
    "parameter_report": "refined_parameter_application_report.json",
    "delta": "refined_original_vs_refined_delta.json",
}
REQUIRED_REFINED_VALIDATION_FILES = {
    "summary": "refined_candidate_actual_path_validation_summary.json",
    "scorecard": "refined_candidate_validation_scorecard.json",
    "high_conviction": "refined_high_conviction_outcome_drilldown.json",
    "false_cost": "refined_false_signal_cost_matrix.json",
    "guardrail": "refined_guardrail_validation_matrix.json",
    "comparison": "original_vs_refined_actual_path_comparison.json",
    "state": "refined_candidate_state_recommendation_matrix.json",
    "data_quality": "refined_candidate_data_quality_report.json",
}
BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}
BANNED_INPUT_TRUE_FIELDS = {
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
}
BANNED_OUTPUT_TRUE_FIELDS = {
    *BANNED_INPUT_TRUE_FIELDS,
    "owner_review_required",
    "actual_path_validation_executed",
    "actual_path_validation_ready",
    "paper_shadow_recommendation_allowed",
    "production_recommendation_allowed",
    "broker_action_recommendation_allowed",
    "promotion_eligible",
}
FALSE_COST_BLOCKING_LABELS = {
    "FALSE_RISK_ON_COST_TOO_HIGH",
    "FALSE_RISK_OFF_COST_TOO_HIGH",
    "BOTH_FALSE_COSTS_TOO_HIGH",
    "FALSE_COST_SCOPE_BLOCKED",
}
HIGH_CONVICTION_BLOCKING_LABELS = {
    "HIGH_CONVICTION_SCOPE_DROP",
    "HIGH_CONVICTION_FALSE_COST_BLOCKED",
}
BASELINE_ALLOWED_DIRECTION_LABELS = {
    "DIRECTION_KEEP",
    "DIRECTION_CONFIRMATION_ONLY",
}
BASELINE_ALLOWED_DIRECTIONS = {
    "risk_on",
    "risk_off",
    "trend_confirming",
    "trend_weakening",
}
VOLATILITY_ALLOWED_DIRECTION_LABELS = {
    "DIRECTION_KEEP",
    "DIRECTION_RISK_CAP_ONLY",
}
VOLATILITY_ALLOWED_DIRECTIONS = {
    "risk_off",
    "trend_weakening",
    "volatility_expansion",
}
# Engineering performance guard for generic schema validation over full trace
# artifacts. Full rows still receive TRADING-2291 custom safety and integrity
# checks below; this cap only avoids duplicating the expensive generic traversal.
VALIDATOR_TRACE_SAMPLE_LIMIT = 512
INACTIVE_REASON_ORDER = (
    "missing_scope_recommendation",
    "outside_kept_asset_scope",
    "outside_kept_horizon_scope",
    "outside_kept_direction_scope",
    "not_high_conviction_scope",
    "false_cost_blocked_scope",
    "usage_role_incompatible",
    "neutral_not_active",
    "risk_on_not_allowed_for_risk_cap",
    "data_quality_scope_blocked",
)


class ScopeNarrowedCandidateRegenerationError(ValueError):
    pass


@dataclass(frozen=True)
class RefinedCandidateArtifacts:
    refined_candidate_id: str
    original_candidate_id: str
    candidate_dir: Path
    signal_spec_path: Path
    signal_series_path: Path
    prediction_artifact_path: Path
    generation_summary_path: Path
    validation_summary_path: Path
    parameter_report_path: Path
    delta_path: Path
    signal_spec: dict[str, Any]
    signal_series_rows: list[dict[str, Any]]
    prediction_artifact: dict[str, Any]
    prediction_records: list[dict[str, Any]]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    parameter_report: dict[str, Any]
    delta: dict[str, Any]


@dataclass(frozen=True)
class ScopeNarrowedRegenerationInputs:
    scope_review_dir: Path
    refined_generator_dir: Path
    refined_validation_dir: Path
    include_candidates: tuple[str, ...]
    archive_candidates: tuple[str, ...]
    scope_review_payloads: dict[str, dict[str, Any]]
    refined_generator_payloads: dict[str, dict[str, Any]]
    refined_validation_payloads: dict[str, dict[str, Any]]
    refined_candidates: dict[str, RefinedCandidateArtifacts]

    @property
    def scope_recommendation_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.scope_review_payloads["scope_recommendation"], "rows")

    @property
    def direction_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.scope_review_payloads["direction_scope"], "rows")

    @property
    def high_conviction_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.scope_review_payloads["high_conviction_scope"], "rows")

    @property
    def false_cost_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.scope_review_payloads["false_cost_scope"], "rows")

    @property
    def state_rows(self) -> list[dict[str, Any]]:
        return _rows_from_payload(self.refined_validation_payloads["state"], "candidate_rows")


@dataclass(frozen=True)
class ScopeNarrowingPlan:
    refined_candidate_id: str
    original_candidate_id: str
    scope_narrowed_candidate_id: str
    usage_role: str
    recommendation: dict[str, Any]
    direction_by_value: dict[str, dict[str, Any]]
    high_conviction: dict[str, Any]
    false_cost_by_dimension_value: dict[tuple[str, str], dict[str, Any]]
    kept_assets: tuple[str, ...]
    dropped_assets: tuple[str, ...]
    kept_horizons: tuple[str, ...]
    dropped_horizons: tuple[str, ...]
    kept_directions: tuple[str, ...]
    dropped_directions: tuple[str, ...]
    kept_regimes: tuple[str, ...]


@dataclass(frozen=True)
class ScopeNarrowedCandidateBundle:
    plan: ScopeNarrowingPlan
    signal_spec: dict[str, Any]
    signal_records: list[dict[str, Any]]
    prediction_artifact: dict[str, Any]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]
    scope_filter_report: dict[str, Any]
    lineage_report: dict[str, Any]
    delta_summary: dict[str, Any]


def run_scope_narrowed_candidate_generators_regenerate(
    *,
    scope_review_dir: Path = DEFAULT_SCOPE_REVIEW_ROOT,
    refined_generator_dir: Path = DEFAULT_REFINED_GENERATOR_ROOT,
    refined_validation_dir: Path = DEFAULT_REFINED_VALIDATION_ROOT,
    include_candidates: Sequence[str] | str = DEFAULT_INCLUDE_CANDIDATES,
    archive_candidates: Sequence[str] | str = DEFAULT_ARCHIVE_CANDIDATES,
    target_assets: Sequence[str] | str = DEFAULT_TARGET_ASSETS,
    horizons: Sequence[str] | str = DEFAULT_HORIZONS,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise ScopeNarrowedCandidateRegenerationError(
            "scope-narrowed candidate regeneration only supports "
            "scope_narrowed_regeneration mode"
        )
    include_ids = _normalize_list(include_candidates)
    archive_ids = _normalize_list(archive_candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)

    inputs = load_scope_narrowed_candidate_regeneration_inputs(
        scope_review_dir=scope_review_dir,
        refined_generator_dir=refined_generator_dir,
        refined_validation_dir=refined_validation_dir,
        include_candidates=include_ids,
        archive_candidates=archive_ids,
    )
    bundles: list[ScopeNarrowedCandidateBundle] = []
    for refined_candidate_id in include_ids:
        plan = build_scope_narrowing_plan(inputs=inputs, refined_candidate_id=refined_candidate_id)
        refined = inputs.refined_candidates[refined_candidate_id]
        source_rows = _filter_records(
            refined.prediction_records,
            target_assets=asset_ids,
            horizons=horizon_ids,
        )
        if not source_rows:
            raise ScopeNarrowedCandidateRegenerationError(
                f"{refined_candidate_id}: no refined prediction records in requested scope"
            )
        source_paths, source_hashes = _scope_source_lineage(
            refined=refined,
            scope_review_dir=scope_review_dir,
        )
        signal_records = build_scope_narrowed_signal_records(
            refined=refined,
            plan=plan,
            source_rows=source_rows,
            source_paths=source_paths,
            source_hashes=source_hashes,
            generated_at=generated_at,
        )
        signal_spec = build_scope_narrowed_signal_spec(
            refined_signal_spec=refined.signal_spec,
            plan=plan,
            generated_at=generated_at,
            source_paths=source_paths,
            source_hashes=source_hashes,
        )
        prediction_artifact = build_scope_narrowed_prediction_artifact(
            refined=refined,
            plan=plan,
            signal_spec=signal_spec,
            signal_records=signal_records,
            generated_at=generated_at,
            source_paths=source_paths,
            source_hashes=source_hashes,
        )
        scope_filter_report = build_scope_filter_report(plan=plan, signal_records=signal_records)
        lineage_report = build_scope_narrowed_lineage_report(
            refined=refined,
            plan=plan,
            source_paths=source_paths,
            source_hashes=source_hashes,
            generated_at=generated_at,
        )
        delta_summary = build_refined_vs_scope_narrowed_delta(
            refined_candidate_id=refined_candidate_id,
            scope_narrowed_candidate_id=plan.scope_narrowed_candidate_id,
            refined_records=source_rows,
            scope_records=signal_records,
        )
        generation_summary = build_scope_narrowed_generation_summary(
            plan=plan,
            signal_records=signal_records,
            generated_at=generated_at,
        )
        validation_summary = validate_scope_narrowed_candidate_artifacts(
            signal_spec=signal_spec,
            signal_records=signal_records,
            prediction_artifact=prediction_artifact,
            scope_filter_report=scope_filter_report,
            lineage_report=lineage_report,
            delta_summary=delta_summary,
            plan=plan,
        )
        if validation_summary["status"] != "PASS":
            raise ScopeNarrowedCandidateRegenerationError(
                f"{refined_candidate_id} validation failed: {validation_summary['errors']}"
            )
        bundles.append(
            ScopeNarrowedCandidateBundle(
                plan=plan,
                signal_spec=signal_spec,
                signal_records=signal_records,
                prediction_artifact=prediction_artifact,
                generation_summary=generation_summary,
                validation_summary=validation_summary,
                scope_filter_report=scope_filter_report,
                lineage_report=lineage_report,
                delta_summary=delta_summary,
            )
        )

    archive_record = build_risk_appetite_current_form_archive_record(
        inputs=inputs,
        archive_candidates=archive_ids,
        generated_at=generated_at,
    )
    registry = build_scope_narrowed_candidate_registry(
        bundles=bundles,
        archive_record=archive_record,
    )
    delta_summary = build_original_vs_refined_vs_scope_delta_summary(
        bundles=bundles,
        refined_generator_payloads=inputs.refined_generator_payloads,
        generated_at=generated_at,
    )
    top_validation = build_scope_narrowed_top_level_validation_summary(
        bundles=bundles,
        archive_record=archive_record,
        generated_at=generated_at,
    )
    run_summary = build_scope_narrowed_regeneration_run_summary(
        inputs=inputs,
        bundles=bundles,
        archive_record=archive_record,
        validation_summary=top_validation,
        generated_at=generated_at,
        mode=mode,
    )

    _write_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        run_summary=run_summary,
        validation_summary=top_validation,
        registry=registry,
        delta_summary=delta_summary,
        bundles=bundles,
        archive_record=archive_record,
    )
    payload = {
        **_common_payload(generated_at=generated_at, mode=mode),
        "summary": run_summary["summary"],
        "validation_summary": top_validation,
        "candidate_registry": registry,
        "generated_candidate_ids": [
            bundle.plan.scope_narrowed_candidate_id for bundle in bundles
        ],
        "archived_candidate_ids": [archive_record["candidate_id"]],
    }
    _assert_generated_payload_safe("scope_narrowed_regeneration", payload)
    return clean_for_yaml(payload)


def load_scope_narrowed_candidate_regeneration_inputs(
    *,
    scope_review_dir: Path,
    refined_generator_dir: Path,
    refined_validation_dir: Path,
    include_candidates: Sequence[str] | str,
    archive_candidates: Sequence[str] | str,
) -> ScopeNarrowedRegenerationInputs:
    include_ids = _normalize_list(include_candidates)
    archive_ids = _normalize_list(archive_candidates)
    for candidate_id in include_ids:
        if candidate_id not in SCOPE_NARROWED_CANDIDATE_MAP:
            raise ScopeNarrowedCandidateRegenerationError(
                f"unsupported included candidate: {candidate_id}"
            )
    if RISK_APPETITE_ARCHIVE_CANDIDATE not in set(archive_ids):
        raise ScopeNarrowedCandidateRegenerationError(
            "risk_appetite_refined_confidence_v1 archive candidate is required"
        )

    scope_payloads = {
        key: _load_json_required(scope_review_dir / filename, f"scope_review.{key}")
        for key, filename in REQUIRED_SCOPE_REVIEW_FILES.items()
    }
    for key, filename in OPTIONAL_SCOPE_REVIEW_FILES.items():
        path = scope_review_dir / filename
        if path.exists():
            scope_payloads[key] = _load_json_required(path, f"scope_review.{key}")

    generator_payloads = {
        key: _load_json_required(refined_generator_dir / filename, f"refined_generator.{key}")
        for key, filename in REQUIRED_REFINED_GENERATOR_TOP_LEVEL_FILES.items()
    }
    refined_candidates = {
        candidate_id: _load_refined_candidate_artifacts(
            refined_generator_dir=refined_generator_dir,
            refined_candidate_id=candidate_id,
        )
        for candidate_id in (*include_ids, *archive_ids)
    }
    validation_payloads = {
        key: _load_json_required(refined_validation_dir / filename, f"refined_validation.{key}")
        for key, filename in REQUIRED_REFINED_VALIDATION_FILES.items()
    }

    inputs = ScopeNarrowedRegenerationInputs(
        scope_review_dir=scope_review_dir,
        refined_generator_dir=refined_generator_dir,
        refined_validation_dir=refined_validation_dir,
        include_candidates=tuple(include_ids),
        archive_candidates=tuple(archive_ids),
        scope_review_payloads=scope_payloads,
        refined_generator_payloads=generator_payloads,
        refined_validation_payloads=validation_payloads,
        refined_candidates=refined_candidates,
    )
    _validate_scope_review_state(inputs)
    _validate_refined_validation_state(inputs)
    return inputs


def build_scope_narrowing_plan(
    *,
    inputs: ScopeNarrowedRegenerationInputs,
    refined_candidate_id: str,
) -> ScopeNarrowingPlan:
    config = SCOPE_NARROWED_CANDIDATE_MAP[refined_candidate_id]
    recommendation = _single_candidate_row(
        inputs.scope_recommendation_rows,
        refined_candidate_id=refined_candidate_id,
        label="scope recommendation",
    )
    if recommendation.get("recommended_scope_action") != "SCOPE_NARROW_AND_REGENERATE":
        raise ScopeNarrowedCandidateRegenerationError(
            f"{refined_candidate_id}: expected SCOPE_NARROW_AND_REGENERATE"
        )
    expected_usage = str(config["usage_role"])
    if recommendation.get("usage_recommendation") != expected_usage:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{refined_candidate_id}: expected usage {expected_usage}"
        )
    direction_rows = [
        row
        for row in inputs.direction_rows
        if str(row.get("refined_candidate_id")) == refined_candidate_id
    ]
    high_row = _single_candidate_row(
        inputs.high_conviction_rows,
        refined_candidate_id=refined_candidate_id,
        label="high conviction scope",
    )
    false_cost_rows = [
        row
        for row in inputs.false_cost_rows
        if str(row.get("refined_candidate_id")) == refined_candidate_id
    ]
    return ScopeNarrowingPlan(
        refined_candidate_id=refined_candidate_id,
        original_candidate_id=str(config["original_candidate_id"]),
        scope_narrowed_candidate_id=str(config["scope_narrowed_candidate_id"]),
        usage_role=expected_usage,
        recommendation=dict(recommendation),
        direction_by_value={str(row.get("signal_direction")): dict(row) for row in direction_rows},
        high_conviction=dict(high_row),
        false_cost_by_dimension_value={
            (str(row.get("scope_dimension")), str(row.get("scope_value"))): dict(row)
            for row in false_cost_rows
        },
        kept_assets=tuple(_strings(recommendation.get("kept_assets"))),
        dropped_assets=tuple(_strings(recommendation.get("dropped_assets"))),
        kept_horizons=tuple(_strings(recommendation.get("kept_horizons"))),
        dropped_horizons=tuple(_strings(recommendation.get("dropped_horizons"))),
        kept_directions=tuple(_strings(recommendation.get("kept_directions"))),
        dropped_directions=tuple(_strings(recommendation.get("dropped_directions"))),
        kept_regimes=tuple(_strings(recommendation.get("kept_regimes"))),
    )


def build_scope_narrowed_signal_records(
    *,
    refined: RefinedCandidateArtifacts,
    plan: ScopeNarrowingPlan,
    source_rows: Sequence[Mapping[str, Any]],
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, source in enumerate(source_rows):
        active, reasons = _scope_active_and_reasons(source, plan)
        row = _scope_narrowed_record_payload(
            source=source,
            row_index=index,
            refined=refined,
            plan=plan,
            active=active,
            reasons=reasons,
            source_paths=source_paths,
            source_hashes=source_hashes,
            generated_at=generated_at,
        )
        rows.append(row)
    return rows


def build_scope_narrowed_signal_spec(
    *,
    refined_signal_spec: Mapping[str, Any],
    plan: ScopeNarrowingPlan,
    generated_at: datetime,
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
) -> dict[str, Any]:
    spec = dict(refined_signal_spec)
    spec.update(
        {
            "candidate_id": plan.scope_narrowed_candidate_id,
            "refined_candidate_id": plan.refined_candidate_id,
            "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
            "scope_source_task": SCOPE_SOURCE_TASK,
            "scope_narrowing_task_id": TASK_ID,
            "scope_narrowing_version": SCOPE_NARROWING_VERSION,
            "generator_id": f"{plan.scope_narrowed_candidate_id}_generator",
            "generator_version": f"{plan.scope_narrowed_candidate_id}.v1",
            "generated_at": generated_at.isoformat(),
            "as_of_timestamp": generated_at.isoformat(),
            "decision_timestamp": generated_at.isoformat(),
            "valid_from": generated_at.isoformat(),
            "valid_until": generated_at.isoformat(),
            "horizon": ",".join(_strings(refined_signal_spec.get("supported_horizons"))),
            "usage_role": plan.usage_role,
            "artifact_role": "candidate_signal_spec",
            "validity_rule": refined_signal_spec.get("validity_rule")
            or "scope_narrowed_active_records_only_are_research_usable",
            "actual_path_validation_ready": False,
            "owner_review_required": False,
            "source_artifact_hash": source_hashes[0],
            "source_refined_artifact_hash": source_hashes[1],
            "source_scope_review_artifact_hash": source_hashes[2],
            "provenance": _scope_provenance(
                plan=plan,
                source_paths=source_paths,
                source_hashes=source_hashes,
            ),
            **generator_operation_safety_fields(),
            "promotion_eligible": False,
            "permanently_inconclusive_override_allowed": False,
        }
    )
    if plan.usage_role == "confirmation_only":
        spec["allowed_usage_roles"] = [
            "confirmation_only",
            "trend_confirmation_gate",
            "trend_weakening_confirmation",
        ]
        spec["disallowed_usage_roles"] = [
            "primary_signal_candidate",
            "risk_cap_only",
            "veto_only",
            "broker_action",
        ]
    else:
        spec["allowed_usage_roles"] = [
            "risk_cap_only",
            "veto_only",
            "exposure_limiter",
            "cooldown_trigger",
        ]
        spec["disallowed_usage_roles"] = [
            "primary_signal_candidate",
            "trend_confirmation_gate",
            "broker_action",
        ]
    return clean_for_yaml(spec)


def build_scope_narrowed_prediction_artifact(
    *,
    refined: RefinedCandidateArtifacts,
    plan: ScopeNarrowingPlan,
    signal_spec: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
) -> dict[str, Any]:
    if not signal_records:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{plan.scope_narrowed_candidate_id}: prediction artifact requires records"
        )
    latest = dict(signal_records[-1])
    active_records = [dict(row) for row in signal_records if _bool(row.get("scope_active"))]
    inactive_records = [dict(row) for row in signal_records if not _bool(row.get("scope_active"))]
    artifact = {
        "schema_version": PREDICTION_SCHEMA_VERSION,
        "artifact_id": f"{plan.scope_narrowed_candidate_id}_prediction_artifact",
        "artifact_role": ARTIFACT_ROLE,
        "candidate_id": plan.scope_narrowed_candidate_id,
        "candidate_family": latest["candidate_family"],
        "original_candidate_id": plan.original_candidate_id,
        "refined_candidate_id": plan.refined_candidate_id,
        "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
        "scope_source_task": SCOPE_SOURCE_TASK,
        "scope_narrowing_task_id": TASK_ID,
        "scope_narrowing_version": SCOPE_NARROWING_VERSION,
        "usage_role": plan.usage_role,
        "source_experiment_id": str(signal_spec.get("generator_id")),
        "source_artifact_id": plan.refined_candidate_id,
        "source_artifact_path": str(refined.prediction_artifact_path),
        "source_artifact_hash": _sha256(refined.prediction_artifact_path),
        "source_refined_artifact_hash": _sha256(refined.prediction_artifact_path),
        "source_scope_review_artifact_hash": source_hashes[2],
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
        "valid_from": latest["valid_from"],
        "valid_until": latest["valid_until"],
        "input_snapshot_hash": latest["input_snapshot_hash"],
        "feature_snapshot_hash": latest["feature_snapshot_hash"],
        "model_or_rule_version": latest["model_or_rule_version"],
        "provenance": _scope_provenance(
            plan=plan,
            source_paths=source_paths,
            source_hashes=source_hashes,
        ),
        "prediction_records": list(signal_records),
        "active_prediction_records": active_records,
        "inactive_prediction_records": inactive_records,
        "source_schema_status": "candidate_bound",
        "record_count": len(signal_records),
        "active_record_count": len(active_records),
        "inactive_record_count": len(inactive_records),
        "generation_mode": MODE,
        "candidate_binding_method": "native_candidate_id",
        "historical_executable_artifact": True,
        "actual_path_validation_ready": False,
        "actual_path_validation_blocker": NEXT_TASK,
        "dynamic_promotion_status": "BLOCKED",
        "owner_review_required": False,
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
        "broker_action_recommendation_allowed": False,
        "regeneration_executed": True,
        "actual_path_validation_executed": False,
        **candidate_artifact_safety_fields(),
        **trading_2281_boundary_fields(),
        "trading_2285_original_inconclusive_decisions_changed": False,
        "trading_2289_refined_state_decisions_changed": False,
    }
    return artifact


def build_scope_filter_report(
    *,
    plan: ScopeNarrowingPlan,
    signal_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    active = [row for row in signal_records if _bool(row.get("scope_active"))]
    inactive = [row for row in signal_records if not _bool(row.get("scope_active"))]
    inactive_reasons = Counter(
        reason
        for row in inactive
        for reason in _strings(row.get("inactive_reasons"))
    )
    source_record_count = len(signal_records)
    return clean_for_yaml(
        {
            **_common_static_payload(),
            "schema_version": "scope_filter_report.v1",
            "task_id": TASK_ID,
            "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
            "refined_candidate_id": plan.refined_candidate_id,
            "original_candidate_id": plan.original_candidate_id,
            "usage_role": plan.usage_role,
            "source_record_count": source_record_count,
            "active_record_count": len(active),
            "inactive_record_count": len(inactive),
            "active_ratio": round_float(_ratio(len(active), source_record_count)),
            "inactive_ratio": round_float(_ratio(len(inactive), source_record_count)),
            "active_by_asset": _count_by(active, "target_asset"),
            "active_by_horizon": _count_by(active, "horizon"),
            "active_by_direction": _count_by(active, "signal_direction"),
            "active_by_high_conviction_flag": _count_by(active, "high_conviction_flag"),
            "inactive_reasons": dict(sorted(inactive_reasons.items())),
            "kept_assets": list(plan.kept_assets),
            "dropped_assets": list(plan.dropped_assets),
            "kept_horizons": list(plan.kept_horizons),
            "dropped_horizons": list(plan.dropped_horizons),
            "kept_directions": list(plan.kept_directions),
            "dropped_directions": list(plan.dropped_directions),
            "kept_regimes": list(plan.kept_regimes),
            "guardrail_blocked_count": inactive_reasons.get("data_quality_scope_blocked", 0),
            "false_cost_blocked_count": inactive_reasons.get("false_cost_blocked_scope", 0),
        }
    )


def build_scope_narrowed_lineage_report(
    *,
    refined: RefinedCandidateArtifacts,
    plan: ScopeNarrowingPlan,
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
    generated_at: datetime,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            **_common_payload(generated_at=generated_at, mode=MODE),
            "schema_version": "scope_narrowed_lineage_report.v1",
            "original_candidate_id": plan.original_candidate_id,
            "refined_candidate_id": plan.refined_candidate_id,
            "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
            "source_refined_artifact_path": str(refined.prediction_artifact_path),
            "source_refined_artifact_hash": _sha256(refined.prediction_artifact_path),
            "source_scope_review_artifact_hash": source_hashes[2],
            "scope_source_task": SCOPE_SOURCE_TASK,
            "scope_narrowing_task_id": TASK_ID,
            "scope_narrowing_version": SCOPE_NARROWING_VERSION,
            "usage_role": plan.usage_role,
            "source_paths": list(source_paths),
            "source_hashes": list(source_hashes),
            "provenance": _scope_provenance(
                plan=plan,
                source_paths=source_paths,
                source_hashes=source_hashes,
            ),
        }
    )


def build_refined_vs_scope_narrowed_delta(
    *,
    refined_candidate_id: str,
    scope_narrowed_candidate_id: str,
    refined_records: Sequence[Mapping[str, Any]],
    scope_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            **_common_static_payload(),
            "schema_version": "refined_vs_scope_narrowed_delta.v1",
            "task_id": TASK_ID,
            "refined_candidate_id": refined_candidate_id,
            "scope_narrowed_candidate_id": scope_narrowed_candidate_id,
            "comparison_scope": (
                "distribution_only_no_actual_path_validation_or_outcome_improvement_claim"
            ),
            "refined_distribution": _distribution_summary(refined_records),
            "scope_narrowed_distribution": _distribution_summary(scope_records),
            "forbidden_comparisons_executed": False,
            "forbidden_comparisons": [
                "forward_return improvement",
                "alignment improvement",
                "actual-path improvement",
                "owner review readiness",
                "paper-shadow readiness",
                "production readiness",
            ],
        }
    )


def build_scope_narrowed_generation_summary(
    *,
    plan: ScopeNarrowingPlan,
    signal_records: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    active_count = sum(1 for row in signal_records if _bool(row.get("scope_active")))
    return clean_for_yaml(
        {
            **_common_payload(generated_at=generated_at, mode=MODE),
            "schema_version": "scope_narrowed_generation_summary.v1",
            "status": STATUS,
            "original_candidate_id": plan.original_candidate_id,
            "refined_candidate_id": plan.refined_candidate_id,
            "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
            "usage_role": plan.usage_role,
            "source_record_count": len(signal_records),
            "active_record_count": active_count,
            "inactive_record_count": len(signal_records) - active_count,
            "scope_filter_version": SCOPE_FILTER_VERSION,
            "actual_path_validation_executed": False,
            "actual_path_validation_ready": False,
            "next_task": NEXT_TASK,
        }
    )


def validate_scope_narrowed_candidate_artifacts(
    *,
    signal_spec: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
    scope_filter_report: Mapping[str, Any],
    lineage_report: Mapping[str, Any],
    delta_summary: Mapping[str, Any],
    plan: ScopeNarrowingPlan,
) -> dict[str, Any]:
    validator = CandidateSignalBindingValidator()
    spec_validation = validator.validate_candidate_signal_spec(signal_spec)
    validator_records = _validator_record_sample(signal_records)
    series_validation = validator.validate_candidate_bound_signal_series(validator_records)
    sampled_artifact = dict(prediction_artifact)
    sampled_artifact["prediction_records"] = validator_records
    artifact_validation = validator.validate_candidate_bound_prediction_artifact(
        sampled_artifact
    )
    errors = (
        list(spec_validation.errors)
        + list(series_validation.errors)
        + list(artifact_validation.errors)
        + _scope_narrowed_integrity_errors(
            signal_spec=signal_spec,
            signal_records=signal_records,
            prediction_artifact=prediction_artifact,
            scope_filter_report=scope_filter_report,
            lineage_report=lineage_report,
            delta_summary=delta_summary,
            plan=plan,
        )
    )
    return clean_for_yaml(
        {
            **_common_static_payload(),
            "schema_version": "scope_narrowed_validation_summary.v1",
            "task_id": TASK_ID,
            "status": "PASS" if not errors else "FAIL",
            "refined_candidate_id": plan.refined_candidate_id,
            "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
            "usage_role": plan.usage_role,
            "signal_spec_validation": spec_validation.to_dict(),
            "signal_series_validation": series_validation.to_dict(),
            "prediction_artifact_validation": artifact_validation.to_dict(),
            "checked_signal_record_count": len(signal_records),
            "checked_prediction_record_count": len(
                prediction_artifact.get("prediction_records", [])
            )
            if isinstance(prediction_artifact.get("prediction_records"), list)
            else 0,
            "scope_filter_report_generated": bool(scope_filter_report),
            "lineage_report_generated": bool(lineage_report),
            "delta_summary_generated": bool(delta_summary),
            "candidate_bound_validator_reused": True,
            "scope_narrowed_fail_closed_checks_satisfied": not errors,
            "errors": errors,
            "actual_path_validation_ready": False,
            "actual_path_validation_executed": False,
        }
    )


def build_risk_appetite_current_form_archive_record(
    *,
    inputs: ScopeNarrowedRegenerationInputs,
    archive_candidates: Sequence[str],
    generated_at: datetime,
) -> dict[str, Any]:
    if RISK_APPETITE_ARCHIVE_CANDIDATE not in set(archive_candidates):
        raise ScopeNarrowedCandidateRegenerationError("risk_appetite archive is required")
    reject_record = inputs.scope_review_payloads["risk_appetite_reject"]
    state_row = _single_candidate_row(
        inputs.state_rows,
        refined_candidate_id=RISK_APPETITE_ARCHIVE_CANDIDATE,
        label="risk_appetite state recommendation",
    )
    if state_row.get("recommended_research_status") != (
        "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
    ):
        raise ScopeNarrowedCandidateRegenerationError(
            "risk_appetite refined candidate is not reject recommended by TRADING-2289"
        )
    if reject_record.get("refined_candidate_id") != RISK_APPETITE_ARCHIVE_CANDIDATE:
        raise ScopeNarrowedCandidateRegenerationError("missing risk_appetite reject record")
    record = {
        **_common_payload(generated_at=generated_at, mode=MODE),
        "schema_version": "risk_appetite_current_form_archive_record.v1",
        "candidate_id": RISK_APPETITE_ARCHIVE_CANDIDATE,
        "original_candidate_id": "risk_appetite",
        "archive_reason": "current_form_reject_recommended_by_2289_and_confirmed_by_2290",
        "archive_scope": "current_form",
        "source_tasks": [
            "TRADING-2284",
            "TRADING-2285",
            "TRADING-2286",
            "TRADING-2287",
            "TRADING-2288",
            "TRADING-2289",
            "TRADING-2290",
        ],
        "source_reject_record": dict(reject_record),
        "future_reopen_policy": {
            "one_of": [
                "revisit_only_with_new_inputs",
                "revisit_only_with_new_candidate_family",
                "archived_no_near_term_iteration",
            ],
            "not_permanent_concept_rejection": True,
        },
        "risk_appetite_regenerated": False,
        "actual_path_validation_executed": False,
        "actual_path_validation_ready": False,
        "next_task": "TRADING-2291_Archive_Rejected_Candidate_Current_Form",
    }
    _assert_generated_payload_safe("risk_appetite archive", record)
    return clean_for_yaml(record)


def build_scope_narrowed_candidate_registry(
    *,
    bundles: Sequence[ScopeNarrowedCandidateBundle],
    archive_record: Mapping[str, Any],
) -> dict[str, Any]:
    rows = []
    for bundle in bundles:
        report = bundle.scope_filter_report
        rows.append(
            {
                **_common_static_payload(),
                "original_candidate_id": bundle.plan.original_candidate_id,
                "refined_candidate_id": bundle.plan.refined_candidate_id,
                "scope_narrowed_candidate_id": bundle.plan.scope_narrowed_candidate_id,
                "usage_role": bundle.plan.usage_role,
                "artifact_role": ARTIFACT_ROLE,
                "source_record_count": report["source_record_count"],
                "active_record_count": report["active_record_count"],
                "inactive_record_count": report["inactive_record_count"],
                "active_ratio": report["active_ratio"],
                "actual_path_validation_ready": False,
                "actual_path_validation_executed": False,
                "next_task": NEXT_TASK,
            }
        )
    return clean_for_yaml(
        {
            **_common_static_payload(),
            "schema_version": "scope_narrowed_candidate_registry.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "rows": rows,
            "archived_candidates": [
                {
                    "candidate_id": archive_record["candidate_id"],
                    "archive_scope": archive_record["archive_scope"],
                    "risk_appetite_regenerated": False,
                    **_common_static_payload(),
                }
            ],
        }
    )


def build_original_vs_refined_vs_scope_delta_summary(
    *,
    bundles: Sequence[ScopeNarrowedCandidateBundle],
    refined_generator_payloads: Mapping[str, Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            **_common_payload(generated_at=generated_at, mode=MODE),
            "schema_version": "scope_narrowed_original_vs_refined_vs_scope_delta_summary.v1",
            "status": STATUS,
            "comparison_scope": "distribution_only",
            "refined_source_delta_summary": dict(
                refined_generator_payloads.get("delta_summary", {})
            ),
            "candidate_rows": [
                {
                    "original_candidate_id": bundle.plan.original_candidate_id,
                    "refined_candidate_id": bundle.plan.refined_candidate_id,
                    "scope_narrowed_candidate_id": bundle.plan.scope_narrowed_candidate_id,
                    "usage_role": bundle.plan.usage_role,
                    "refined_distribution": bundle.delta_summary["refined_distribution"],
                    "scope_narrowed_distribution": bundle.delta_summary[
                        "scope_narrowed_distribution"
                    ],
                }
                for bundle in bundles
            ],
            "actual_path_validation_executed": False,
            "forbidden_outcome_comparison_executed": False,
        }
    )


def build_scope_narrowed_top_level_validation_summary(
    *,
    bundles: Sequence[ScopeNarrowedCandidateBundle],
    archive_record: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    errors = [
        error
        for bundle in bundles
        for error in _strings(bundle.validation_summary.get("errors"))
    ]
    if archive_record.get("risk_appetite_regenerated") is not False:
        errors.append("risk_appetite regenerated instead of archived")
    return clean_for_yaml(
        {
            **_common_payload(generated_at=generated_at, mode=MODE),
            "schema_version": "scope_narrowed_regeneration_validation_summary.v1",
            "status": "PASS" if not errors else "FAIL",
            "candidate_validation_rows": [
                bundle.validation_summary for bundle in bundles
            ],
            "archive_record_generated": bool(archive_record),
            "risk_appetite_regenerated": False,
            "checked_candidate_count": len(bundles),
            "errors": errors,
            "actual_path_validation_executed": False,
            "actual_path_validation_ready": False,
        }
    )


def build_scope_narrowed_regeneration_run_summary(
    *,
    inputs: ScopeNarrowedRegenerationInputs,
    bundles: Sequence[ScopeNarrowedCandidateBundle],
    archive_record: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    generated_at: datetime,
    mode: str,
) -> dict[str, Any]:
    summary = {
        "task_id": TASK_ID,
        "status": STATUS,
        "input_scope_review_task": SCOPE_SOURCE_TASK,
        "input_scope_review_dir": str(inputs.scope_review_dir),
        "input_refined_generator_dir": str(inputs.refined_generator_dir),
        "input_refined_validation_dir": str(inputs.refined_validation_dir),
        "included_candidate_count": len(bundles),
        "archived_candidate_count": 1,
        "generated_scope_narrowed_candidate_ids": [
            bundle.plan.scope_narrowed_candidate_id for bundle in bundles
        ],
        "archived_candidate_ids": [archive_record["candidate_id"]],
        "validation_status": validation_summary.get("status"),
        "actual_path_validation_executed": False,
        "actual_path_validation_ready": False,
        "next_task": NEXT_TASK,
        "trading_2281_permanently_inconclusive_decisions_changed": False,
        "trading_2285_original_inconclusive_decisions_changed": False,
        "trading_2289_refined_state_decisions_changed": False,
    }
    return clean_for_yaml(
        {
            **_common_payload(generated_at=generated_at, mode=mode),
            "schema_version": "scope_narrowed_regeneration_run_summary.v1",
            "status": STATUS,
            "summary": summary,
        }
    )


def _scope_active_and_reasons(
    row: Mapping[str, Any],
    plan: ScopeNarrowingPlan,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    direction = str(row.get("signal_direction") or "")
    asset = str(row.get("target_asset") or "")
    horizon = str(row.get("horizon") or "")
    direction_row = plan.direction_by_value.get(direction)

    if not direction_row:
        reasons.append("missing_scope_recommendation")
    if plan.kept_assets and asset not in set(plan.kept_assets):
        reasons.append("outside_kept_asset_scope")
    if plan.kept_horizons and horizon not in set(plan.kept_horizons):
        reasons.append("outside_kept_horizon_scope")
    if direction == "neutral":
        reasons.append("neutral_not_active")
    if plan.kept_directions and direction not in set(plan.kept_directions):
        reasons.append("outside_kept_direction_scope")

    high_label = str(plan.high_conviction.get("high_conviction_scope_label") or "")
    if high_label in HIGH_CONVICTION_BLOCKING_LABELS:
        reasons.append("not_high_conviction_scope")
    if high_label == "HIGH_CONVICTION_SCOPE_KEEP_ONLY" and not _bool(
        row.get("high_conviction_flag")
    ):
        reasons.append("not_high_conviction_scope")

    if _false_cost_blocked(row=row, plan=plan):
        reasons.append("false_cost_blocked_scope")

    direction_label = str((direction_row or {}).get("direction_scope_label") or "")
    if plan.usage_role == "confirmation_only":
        if direction_label not in BASELINE_ALLOWED_DIRECTION_LABELS:
            reasons.append("outside_kept_direction_scope")
        if direction not in BASELINE_ALLOWED_DIRECTIONS:
            reasons.append("usage_role_incompatible")
    elif plan.usage_role == "risk_cap_only":
        if direction == "risk_on":
            reasons.append("risk_on_not_allowed_for_risk_cap")
        if direction == "volatility_compression":
            reasons.append("usage_role_incompatible")
        if direction_label not in VOLATILITY_ALLOWED_DIRECTION_LABELS:
            reasons.append("outside_kept_direction_scope")
        if direction not in VOLATILITY_ALLOWED_DIRECTIONS:
            reasons.append("usage_role_incompatible")
    if not reasons:
        return True, []
    return False, _dedupe_reasons(reasons)


def _scope_narrowed_record_payload(
    *,
    source: Mapping[str, Any],
    row_index: int,
    refined: RefinedCandidateArtifacts,
    plan: ScopeNarrowingPlan,
    active: bool,
    reasons: Sequence[str],
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
    generated_at: datetime,
) -> dict[str, Any]:
    row = dict(source)
    signal_value = to_float(
        row.get("refined_signal_value"),
        default=to_float(row.get("signal_value")),
    )
    signal_confidence = to_float(
        row.get("refined_signal_confidence"),
        default=to_float(row.get("signal_confidence")),
    )
    row.update(
        {
            "candidate_id": plan.scope_narrowed_candidate_id,
            "source_experiment_id": f"{plan.scope_narrowed_candidate_id}_generator",
            "source_artifact_id": plan.scope_narrowed_candidate_id,
            "source_artifact_path": str(refined.signal_series_path),
            "source_artifact_hash": source_hashes[0],
            "generated_at": generated_at.isoformat(),
            "signal_spec_version": SIGNAL_SPEC_VERSION,
            "prediction_schema_version": PREDICTION_SCHEMA_VERSION,
            "signal_value": round_float(signal_value),
            "signal_confidence": round_float(signal_confidence),
            "model_or_rule_version": (
                f"{row.get('model_or_rule_version', plan.original_candidate_id)}."
                f"{SCOPE_NARROWING_VERSION}"
            ),
            "provenance": _scope_row_provenance(
                plan=plan,
                source_paths=source_paths,
                source_hashes=source_hashes,
            ),
            "original_candidate_id": plan.original_candidate_id,
            "refined_candidate_id": plan.refined_candidate_id,
            "scope_narrowed_candidate_id": plan.scope_narrowed_candidate_id,
            "scope_source_task": SCOPE_SOURCE_TASK,
            "scope_narrowing_task_id": TASK_ID,
            "scope_narrowing_version": SCOPE_NARROWING_VERSION,
            "source_refined_artifact_hash": source_hashes[1],
            "source_scope_review_artifact_hash": source_hashes[2],
            "scope_active": bool(active),
            "scope_reason": "active_scope_match" if active else _primary_inactive_reason(reasons),
            "inactive_reasons": list(reasons),
            "usage_role": plan.usage_role,
            "scope_filter_version": SCOPE_FILTER_VERSION,
            "scope_dimension": "direction",
            "scope_value": str(row.get("signal_direction") or ""),
            "source_refined_record_id": _source_refined_record_id(source, row_index),
            "artifact_role": ARTIFACT_ROLE,
            "actual_path_validation_ready": False,
            "actual_path_validation_executed": False,
            "owner_review_required": False,
            "paper_shadow_recommendation_allowed": False,
            "production_recommendation_allowed": False,
            "broker_action_recommendation_allowed": False,
            **candidate_artifact_safety_fields(),
            **trading_2281_boundary_fields(),
            "trading_2285_original_inconclusive_decisions_changed": False,
            "trading_2289_refined_state_decisions_changed": False,
        }
    )
    if plan.usage_role == "confirmation_only":
        row.update(_confirmation_recoding(row, active=active))
    else:
        row.update(_risk_cap_recoding(row, active=active))
    return row


def _confirmation_recoding(row: Mapping[str, Any], *, active: bool) -> dict[str, Any]:
    direction = str(row.get("signal_direction") or "")
    if not active:
        return {
            "confirmation_score": 0.0,
            "confirmation_direction": "inactive",
            "confirmation_confidence": 0.0,
        }
    confirmation_direction = (
        "positive_confirmation"
        if direction in {"trend_confirming", "risk_on"}
        else "negative_confirmation"
    )
    return {
        "confirmation_score": round_float(to_float(row.get("signal_value"))),
        "confirmation_direction": confirmation_direction,
        "confirmation_confidence": round_float(to_float(row.get("signal_confidence"))),
    }


def _risk_cap_recoding(row: Mapping[str, Any], *, active: bool) -> dict[str, Any]:
    direction = str(row.get("signal_direction") or "")
    if not active:
        return {
            "risk_cap_score": 0.0,
            "risk_cap_intensity": "inactive",
            "risk_cap_reason": "inactive_scope",
        }
    intensity = "high" if direction == "volatility_expansion" else "medium"
    if _bool(row.get("high_conviction_flag")) and direction == "risk_off":
        intensity = "medium_high"
    return {
        "risk_cap_score": round_float(abs(to_float(row.get("signal_value")))),
        "risk_cap_intensity": intensity,
        "risk_cap_reason": f"{direction}_scope_active",
    }


def _false_cost_blocked(*, row: Mapping[str, Any], plan: ScopeNarrowingPlan) -> bool:
    checks = [
        ("asset", str(row.get("target_asset") or "")),
        ("horizon", str(row.get("horizon") or "")),
        ("direction", str(row.get("signal_direction") or "")),
        ("high_conviction", "high_conviction_only"),
    ]
    regime = row.get("regime_label") or row.get("source_regime_label")
    if regime:
        checks.append(("regime", str(regime)))
    for key in checks:
        label = str(plan.false_cost_by_dimension_value.get(key, {}).get("false_cost_label") or "")
        if label in FALSE_COST_BLOCKING_LABELS:
            return True
        if "TOO_HIGH" in label or "BLOCKED" in label:
            return True
    return False


def _scope_provenance(
    *,
    plan: ScopeNarrowingPlan,
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
) -> dict[str, Any]:
    return {
        "source_paths": list(source_paths),
        "source_hashes": list(source_hashes),
        "regeneration_mode": REGENERATION_MODE,
        "pit_policy": "strict_pit"
        if plan.original_candidate_id == "baseline_plus_trend_structure"
        else "pit_approximation",
        "candidate_binding_method": "native_candidate_id",
        "source_schema_status": "candidate_bound",
        "promotion_eligible": False,
        "scope_narrowing_source": {
            "task_id": SCOPE_SOURCE_TASK,
            "recommendation_id": (
                f"{plan.refined_candidate_id}:"
                f"{plan.recommendation.get('recommended_scope_action')}"
            ),
            "usage_role": plan.usage_role,
            "kept_assets": list(plan.kept_assets),
            "kept_horizons": list(plan.kept_horizons),
            "kept_directions": list(plan.kept_directions),
            "kept_regimes": list(plan.kept_regimes),
        },
        "scope_narrowing_task_id": TASK_ID,
        "scope_narrowing_version": SCOPE_NARROWING_VERSION,
    }


def _scope_row_provenance(
    *,
    plan: ScopeNarrowingPlan,
    source_paths: Sequence[str],
    source_hashes: Sequence[str],
) -> dict[str, Any]:
    return {
        "source_paths": [source_paths[1], source_paths[2]],
        "source_hashes": [source_hashes[1], source_hashes[2]],
        "regeneration_mode": REGENERATION_MODE,
        "pit_policy": "strict_pit"
        if plan.original_candidate_id == "baseline_plus_trend_structure"
        else "pit_approximation",
        "candidate_binding_method": "native_candidate_id",
        "source_schema_status": "candidate_bound",
        "promotion_eligible": False,
        "scope_narrowing_source": {
            "task_id": SCOPE_SOURCE_TASK,
            "recommendation_id": (
                f"{plan.refined_candidate_id}:"
                f"{plan.recommendation.get('recommended_scope_action')}"
            ),
            "usage_role": plan.usage_role,
        },
        "scope_narrowing_task_id": TASK_ID,
        "scope_narrowing_version": SCOPE_NARROWING_VERSION,
    }


def _scope_narrowed_integrity_errors(
    *,
    signal_spec: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
    scope_filter_report: Mapping[str, Any],
    lineage_report: Mapping[str, Any],
    delta_summary: Mapping[str, Any],
    plan: ScopeNarrowingPlan,
) -> list[str]:
    errors: list[str] = []
    if plan.scope_narrowed_candidate_id == plan.refined_candidate_id:
        errors.append("scope_narrowed_candidate_id equals refined_candidate_id")
    for scope, payload in (
        ("scope_narrowed_signal_spec", signal_spec),
        ("scope_narrowed_prediction_artifact", prediction_artifact),
    ):
        _check_scope_payload(scope, payload, plan, errors)
    if prediction_artifact.get("artifact_role") != ARTIFACT_ROLE:
        errors.append("scope_narrowed_prediction_artifact: invalid artifact_role")
    for field in ("prediction_records", "active_prediction_records", "inactive_prediction_records"):
        if not isinstance(prediction_artifact.get(field), list):
            errors.append(f"scope_narrowed_prediction_artifact: missing {field}")
    if not scope_filter_report:
        errors.append("scope_filter_report missing")
    if not lineage_report:
        errors.append("lineage_report missing")
    if not delta_summary:
        errors.append("delta_summary missing")
    for index, row in enumerate(_validator_record_sample(signal_records)):
        _check_scope_payload(f"scope_narrowed_signal_series_sample[{index}]", row, plan, errors)
    for index, row in enumerate(signal_records):
        scope = f"scope_narrowed_signal_series[{index}]"
        _check_scope_row_fast(scope, row, plan, errors)
        for field in _scope_numeric_fields(plan.usage_role):
            value = to_float(row.get(field), default=float("nan"))
            if not math.isfinite(value):
                errors.append(f"{scope}: {field} must be finite")
    if plan.original_candidate_id == "risk_appetite":
        errors.append("risk_appetite regenerated instead of archived")
    return errors


def _check_scope_payload(
    scope: str,
    payload: Mapping[str, Any],
    plan: ScopeNarrowingPlan,
    errors: list[str],
) -> None:
    required = (
        "candidate_id",
        "original_candidate_id",
        "refined_candidate_id",
        "scope_narrowed_candidate_id",
        "source_artifact_hash",
        "source_refined_artifact_hash",
        "source_scope_review_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "valid_from",
        "valid_until",
        "horizon",
        "signal_spec_version",
        "prediction_schema_version",
        "provenance",
        "scope_source_task",
        "scope_narrowing_task_id",
    )
    for field in required:
        if _is_missing(payload.get(field)):
            errors.append(f"{scope}: missing {field}")
    if payload.get("candidate_id") != plan.scope_narrowed_candidate_id:
        errors.append(f"{scope}: candidate_id must be scope-narrowed candidate id")
    if payload.get("refined_candidate_id") != plan.refined_candidate_id:
        errors.append(f"{scope}: refined_candidate_id mismatch")
    if payload.get("original_candidate_id") != plan.original_candidate_id:
        errors.append(f"{scope}: original_candidate_id mismatch")
    if payload.get("scope_narrowed_candidate_id") != plan.scope_narrowed_candidate_id:
        errors.append(f"{scope}: scope_narrowed_candidate_id mismatch")
    if payload.get("scope_source_task") != SCOPE_SOURCE_TASK:
        errors.append(f"{scope}: scope_source_task must be {SCOPE_SOURCE_TASK}")
    if payload.get("scope_narrowing_task_id") != TASK_ID:
        errors.append(f"{scope}: scope_narrowing_task_id must be {TASK_ID}")
    if str(payload.get("broker_action") or "") != "none":
        errors.append(f"{scope}: broker_action must be none")
    for field in BANNED_OUTPUT_TRUE_FIELDS:
        if _bool(payload.get(field)):
            errors.append(f"{scope}: {field} must be false")
    provenance = _provenance(payload)
    if provenance.get("regeneration_mode") != REGENERATION_MODE:
        errors.append(f"{scope}: provenance.regeneration_mode mismatch")
    if _mapping(provenance.get("scope_narrowing_source")).get("task_id") != SCOPE_SOURCE_TASK:
        errors.append(f"{scope}: missing scope_narrowing_source.task_id")


def _check_scope_row_fast(
    scope: str,
    payload: Mapping[str, Any],
    plan: ScopeNarrowingPlan,
    errors: list[str],
) -> None:
    for field in (
        "candidate_id",
        "original_candidate_id",
        "refined_candidate_id",
        "scope_narrowed_candidate_id",
        "source_artifact_hash",
        "source_refined_artifact_hash",
        "source_scope_review_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "valid_from",
        "valid_until",
        "horizon",
        "signal_spec_version",
        "prediction_schema_version",
        "scope_source_task",
        "scope_narrowing_task_id",
    ):
        if _is_missing(payload.get(field)):
            errors.append(f"{scope}: missing {field}")
    if payload.get("candidate_id") != plan.scope_narrowed_candidate_id:
        errors.append(f"{scope}: candidate_id must be scope-narrowed candidate id")
    if payload.get("refined_candidate_id") != plan.refined_candidate_id:
        errors.append(f"{scope}: refined_candidate_id mismatch")
    if payload.get("original_candidate_id") != plan.original_candidate_id:
        errors.append(f"{scope}: original_candidate_id mismatch")
    if str(payload.get("broker_action") or "") != "none":
        errors.append(f"{scope}: broker_action must be none")
    for field in BANNED_OUTPUT_TRUE_FIELDS:
        if _bool(payload.get(field)):
            errors.append(f"{scope}: {field} must be false")


def _validate_scope_review_state(inputs: ScopeNarrowedRegenerationInputs) -> None:
    scope_by_candidate = {
        str(row.get("refined_candidate_id")): row
        for row in inputs.scope_recommendation_rows
    }
    for candidate_id in inputs.include_candidates:
        if candidate_id not in scope_by_candidate:
            raise ScopeNarrowedCandidateRegenerationError(
                f"{candidate_id}: missing scope recommendation"
            )
        if scope_by_candidate[candidate_id].get("recommended_scope_action") != (
            "SCOPE_NARROW_AND_REGENERATE"
        ):
            raise ScopeNarrowedCandidateRegenerationError(
                f"{candidate_id}: scope action must be SCOPE_NARROW_AND_REGENERATE"
            )
    reject = inputs.scope_review_payloads["risk_appetite_reject"]
    if reject.get("refined_candidate_id") != RISK_APPETITE_ARCHIVE_CANDIDATE:
        raise ScopeNarrowedCandidateRegenerationError("missing risk_appetite reject record")


def _validate_refined_validation_state(inputs: ScopeNarrowedRegenerationInputs) -> None:
    states = {
        str(row.get("refined_candidate_id")): row
        for row in inputs.state_rows
    }
    for candidate_id in inputs.include_candidates:
        state = states.get(candidate_id)
        if not state:
            raise ScopeNarrowedCandidateRegenerationError(
                f"{candidate_id}: missing refined validation state"
            )
        if state.get("recommended_research_status") != (
            "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"
        ):
            raise ScopeNarrowedCandidateRegenerationError(
                f"{candidate_id}: refined validation state is not continue research"
            )
        if _bool(state.get("owner_review_candidate_recommendation")):
            raise ScopeNarrowedCandidateRegenerationError(
                f"{candidate_id}: owner review recommendation must be false"
            )
    archive_state = states.get(RISK_APPETITE_ARCHIVE_CANDIDATE)
    if not archive_state or archive_state.get("recommended_research_status") != (
        "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
    ):
        raise ScopeNarrowedCandidateRegenerationError(
            "risk_appetite refined validation state is not reject recommended"
        )


def _load_refined_candidate_artifacts(
    *,
    refined_generator_dir: Path,
    refined_candidate_id: str,
) -> RefinedCandidateArtifacts:
    candidate_dir = refined_generator_dir / refined_candidate_id
    paths = {
        key: candidate_dir / filename
        for key, filename in REQUIRED_REFINED_CANDIDATE_FILES.items()
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{refined_candidate_id}: missing refined generator artifact(s): {missing}"
        )
    signal_spec = _load_json_required(
        paths["signal_spec"], f"refined_candidate.{refined_candidate_id}.signal_spec"
    )
    prediction_artifact = _read_json_object(paths["prediction_artifact"])
    generation_summary = _load_json_required(
        paths["generation_summary"],
        f"refined_candidate.{refined_candidate_id}.generation_summary",
    )
    validation_summary = _load_json_required(
        paths["validation_summary"],
        f"refined_candidate.{refined_candidate_id}.validation_summary",
    )
    parameter_report = _load_json_required(
        paths["parameter_report"],
        f"refined_candidate.{refined_candidate_id}.parameter_report",
    )
    delta = _load_json_required(paths["delta"], f"refined_candidate.{refined_candidate_id}.delta")
    signal_series_rows = _read_signal_series_csv(paths["signal_series"])
    prediction_records = _records(prediction_artifact.get("prediction_records"))
    if not prediction_records:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{refined_candidate_id}: refined prediction artifact has no prediction_records"
        )
    original_candidate_id = str(
        prediction_artifact.get("original_candidate_id")
        or signal_spec.get("original_candidate_id")
        or ""
    )
    _validate_refined_candidate_artifact_contract(
        refined_candidate_id=refined_candidate_id,
        original_candidate_id=original_candidate_id,
        signal_spec=signal_spec,
        signal_series_rows=signal_series_rows,
        prediction_artifact=prediction_artifact,
        generation_summary=generation_summary,
        validation_summary=validation_summary,
    )
    return RefinedCandidateArtifacts(
        refined_candidate_id=refined_candidate_id,
        original_candidate_id=original_candidate_id,
        candidate_dir=candidate_dir,
        signal_spec_path=paths["signal_spec"],
        signal_series_path=paths["signal_series"],
        prediction_artifact_path=paths["prediction_artifact"],
        generation_summary_path=paths["generation_summary"],
        validation_summary_path=paths["validation_summary"],
        parameter_report_path=paths["parameter_report"],
        delta_path=paths["delta"],
        signal_spec=signal_spec,
        signal_series_rows=signal_series_rows,
        prediction_artifact=prediction_artifact,
        prediction_records=prediction_records,
        generation_summary=generation_summary,
        validation_summary=validation_summary,
        parameter_report=parameter_report,
        delta=delta,
    )


def _validate_refined_candidate_artifact_contract(
    *,
    refined_candidate_id: str,
    original_candidate_id: str,
    signal_spec: Mapping[str, Any],
    signal_series_rows: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
    generation_summary: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
) -> None:
    expected_original = SCOPE_NARROWED_CANDIDATE_MAP.get(refined_candidate_id, {}).get(
        "original_candidate_id"
    )
    if refined_candidate_id == RISK_APPETITE_ARCHIVE_CANDIDATE:
        expected_original = "risk_appetite"
    if expected_original and original_candidate_id != expected_original:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{refined_candidate_id}: original_candidate_id mismatch"
        )
    for scope, payload in (
        ("signal_spec", signal_spec),
        ("prediction_artifact", prediction_artifact),
        ("generation_summary", generation_summary),
        ("validation_summary", validation_summary),
    ):
        if scope == "prediction_artifact":
            _assert_refined_prediction_artifact_safe(refined_candidate_id, payload)
        else:
            _assert_input_payload_safe(f"{refined_candidate_id}.{scope}", payload)
    validator = CandidateSignalBindingValidator()
    validator_series_rows = _validator_record_sample(signal_series_rows)
    validator_prediction_records = _validator_record_sample(
        _records(prediction_artifact.get("prediction_records"))
    )
    series_validation = validator.validate_candidate_bound_signal_series(validator_series_rows)
    sampled_artifact = dict(prediction_artifact)
    sampled_artifact["prediction_records"] = validator_prediction_records
    artifact_validation = validator.validate_candidate_bound_prediction_artifact(
        sampled_artifact
    )
    errors = list(series_validation.errors) + list(artifact_validation.errors)
    if signal_spec.get("candidate_id") != refined_candidate_id:
        errors.append("signal_spec: candidate_id mismatch")
    if prediction_artifact.get("candidate_id") != refined_candidate_id:
        errors.append("prediction_artifact: candidate_id mismatch")
    if prediction_artifact.get("refined_candidate_id") != refined_candidate_id:
        errors.append("prediction_artifact: refined_candidate_id mismatch")
    for field in (
        "candidate_id",
        "original_candidate_id",
        "source_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "valid_from",
        "valid_until",
        "horizon",
        "provenance",
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "broker_action",
    ):
        if _is_missing(prediction_artifact.get(field)):
            errors.append(f"prediction_artifact: missing {field}")
    for index, record in enumerate(prediction_artifact.get("prediction_records", [])):
        if not isinstance(record, Mapping):
            errors.append(f"prediction_records[{index}]: record is not object")
            continue
        for field in (
            "candidate_id",
            "original_candidate_id",
            "source_artifact_hash",
            "as_of_timestamp",
            "decision_timestamp",
            "valid_from",
            "valid_until",
            "horizon",
            "provenance",
            "promotion_allowed",
            "paper_shadow_allowed",
            "production_allowed",
            "broker_action",
        ):
            if _is_missing(record.get(field)):
                errors.append(f"prediction_records[{index}]: missing {field}")
    if errors:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{refined_candidate_id}: refined artifact validation failed: {errors}"
        )


def _write_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    run_summary: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    registry: Mapping[str, Any],
    delta_summary: Mapping[str, Any],
    bundles: Sequence[ScopeNarrowedCandidateBundle],
    archive_record: Mapping[str, Any],
) -> None:
    write_json(output_dir / "scope_narrowed_regeneration_run_summary.json", run_summary)
    write_json(
        output_dir / "scope_narrowed_regeneration_validation_summary.json",
        validation_summary,
    )
    write_json(output_dir / "scope_narrowed_candidate_registry.json", registry)
    write_json(
        output_dir / "scope_narrowed_original_vs_refined_vs_scope_delta_summary.json",
        delta_summary,
    )
    for bundle in bundles:
        candidate_dir = output_dir / bundle.plan.scope_narrowed_candidate_id
        write_json(candidate_dir / "scope_narrowed_candidate_signal_spec.json", bundle.signal_spec)
        write_scope_narrowed_signal_series_csv(
            candidate_dir / "scope_narrowed_candidate_signal_series.csv",
            bundle.signal_records,
        )
        write_json(
            candidate_dir / "scope_narrowed_candidate_prediction_artifact.json",
            bundle.prediction_artifact,
        )
        write_json(
            candidate_dir / "scope_narrowed_generation_summary.json",
            bundle.generation_summary,
        )
        write_json(
            candidate_dir / "scope_narrowed_validation_summary.json",
            bundle.validation_summary,
        )
        write_json(candidate_dir / "scope_filter_report.json", bundle.scope_filter_report)
        write_json(candidate_dir / "scope_narrowed_lineage_report.json", bundle.lineage_report)
        write_json(candidate_dir / "refined_vs_scope_narrowed_delta.json", bundle.delta_summary)

    archive_dir = output_dir / "risk_appetite_archive"
    write_json(
        archive_dir / "risk_appetite_current_form_archive_record.json",
        archive_record,
    )
    write_markdown(
        archive_dir / "risk_appetite_current_form_archive_record.md",
        _render_risk_appetite_archive_doc(archive_record),
    )
    write_markdown(
        docs_root / "scope_narrowed_candidate_regeneration_report.md",
        _render_regeneration_report(run_summary, registry),
    )
    write_markdown(
        docs_root / "scope_narrowed_candidate_scope_filter_report.md",
        _render_scope_filter_doc(bundles),
    )
    write_markdown(
        docs_root / "scope_narrowed_original_vs_refined_delta_summary.md",
        _render_delta_doc(delta_summary),
    )
    write_markdown(
        docs_root / "risk_appetite_current_form_archive_record.md",
        _render_risk_appetite_archive_doc(archive_record),
    )


def write_scope_narrowed_signal_series_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    required = list(candidate_bound_signal_series_contract_dict()["required_columns"])
    extra_fields = [
        "original_candidate_id",
        "refined_candidate_id",
        "scope_narrowed_candidate_id",
        "scope_source_task",
        "scope_narrowing_task_id",
        "scope_narrowing_version",
        "source_refined_artifact_hash",
        "source_scope_review_artifact_hash",
        "scope_active",
        "scope_reason",
        "inactive_reasons",
        "usage_role",
        "scope_filter_version",
        "scope_dimension",
        "scope_value",
        "source_refined_record_id",
        "confirmation_score",
        "confirmation_direction",
        "confirmation_confidence",
        "risk_cap_score",
        "risk_cap_intensity",
        "risk_cap_reason",
        "actual_path_validation_ready",
        "actual_path_validation_executed",
    ]
    seen = set(required)
    fieldnames = list(required)
    for field in extra_fields:
        if field not in seen:
            fieldnames.append(field)
            seen.add(field)
    for row in rows:
        for field in row:
            if field not in seen and field != "prediction_fields":
                fieldnames.append(field)
                seen.add(field)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field)) for field in fieldnames})


def _scope_source_lineage(
    *,
    refined: RefinedCandidateArtifacts,
    scope_review_dir: Path,
) -> tuple[list[str], list[str]]:
    source_paths = [
        refined.signal_series_path,
        refined.prediction_artifact_path,
        scope_review_dir / REQUIRED_SCOPE_REVIEW_FILES["scope_recommendation"],
        scope_review_dir / REQUIRED_SCOPE_REVIEW_FILES["direction_scope"],
        scope_review_dir / REQUIRED_SCOPE_REVIEW_FILES["high_conviction_scope"],
        scope_review_dir / REQUIRED_SCOPE_REVIEW_FILES["false_cost_scope"],
    ]
    return [str(path) for path in source_paths], [_sha256(path) for path in source_paths]


def _distribution_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    record_count = len(records)
    active_records = [row for row in records if _bool(row.get("scope_active"))]
    active_count = len(active_records) if active_records else record_count
    neutral_count = sum(1 for row in records if str(row.get("signal_direction")) == "neutral")
    risk_on_count = sum(1 for row in records if str(row.get("signal_direction")) == "risk_on")
    risk_off_count = sum(1 for row in records if str(row.get("signal_direction")) == "risk_off")
    trend_confirming_count = sum(
        1 for row in records if str(row.get("signal_direction")) == "trend_confirming"
    )
    trend_weakening_count = sum(
        1 for row in records if str(row.get("signal_direction")) == "trend_weakening"
    )
    vol_expansion_count = sum(
        1 for row in records if str(row.get("signal_direction")) == "volatility_expansion"
    )
    vol_compression_count = sum(
        1 for row in records if str(row.get("signal_direction")) == "volatility_compression"
    )
    high_count = sum(1 for row in records if _bool(row.get("high_conviction_flag")))
    return {
        "record_count": record_count,
        "active_record_count": active_count,
        "active_ratio": round_float(_ratio(active_count, record_count)),
        "neutral_ratio": round_float(_ratio(neutral_count, record_count)),
        "directional_signal_ratio": round_float(_ratio(record_count - neutral_count, record_count)),
        "high_confidence_ratio": round_float(_ratio(high_count, record_count)),
        "high_conviction_ratio": round_float(_ratio(high_count, record_count)),
        "risk_on_count": risk_on_count,
        "risk_off_count": risk_off_count,
        "trend_confirming_count": trend_confirming_count,
        "trend_weakening_count": trend_weakening_count,
        "volatility_expansion_count": vol_expansion_count,
        "volatility_compression_count": vol_compression_count,
        "direction_counts": _count_by(records, "signal_direction"),
    }


def _common_payload(*, generated_at: datetime, mode: str) -> dict[str, Any]:
    return {
        **_common_static_payload(),
        "schema_version": "scope_narrowed_candidate_regeneration.v1",
        "task_id": TASK_ID,
        "status": STATUS,
        "mode": mode,
        "generated_at": generated_at.isoformat(),
        "report_type": "scope_narrowed_candidate_regeneration",
        "title": "Scope-Narrowed Candidate Regeneration",
    }


def _common_static_payload() -> dict[str, Any]:
    return {
        "artifact_role": ARTIFACT_ROLE,
        "research_only": True,
        "promotion_eligible": False,
        "owner_review_required": False,
        "paper_shadow_recommendation_allowed": False,
        "production_recommendation_allowed": False,
        "broker_action_recommendation_allowed": False,
        **generator_operation_safety_fields(),
        **trading_2281_boundary_fields(),
        "trading_2285_original_inconclusive_decisions_changed": False,
        "trading_2289_refined_state_decisions_changed": False,
    }


def _load_json_required(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        raise ScopeNarrowedCandidateRegenerationError(f"missing {label}: {path}")
    result = _read_json_object(path)
    _assert_input_payload_safe(label, result)
    return result


def _read_json_object(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        raise ScopeNarrowedCandidateRegenerationError(f"JSON must be object: {path}")
    return dict(payload)


def _assert_refined_prediction_artifact_safe(
    refined_candidate_id: str,
    payload: Mapping[str, Any],
) -> None:
    shallow = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "prediction_records",
            "active_prediction_records",
            "inactive_prediction_records",
        }
    }
    _assert_input_payload_safe(f"{refined_candidate_id}.prediction_artifact", shallow)
    for index, record in enumerate(_records(payload.get("prediction_records"))):
        for field in BANNED_INPUT_TRUE_FIELDS:
            if _bool(record.get(field)):
                raise ScopeNarrowedCandidateRegenerationError(
                    f"{refined_candidate_id}.prediction_records[{index}]: {field} must be false"
                )
        if str(record.get("broker_action") or "") != "none":
            raise ScopeNarrowedCandidateRegenerationError(
                f"{refined_candidate_id}.prediction_records[{index}]: broker_action must be none"
            )


def _read_signal_series_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            parsed = dict(row)
            for field in (
                "provenance",
                "source_prediction_flags",
                "selected_proposal_ids",
                "selected_parameter_set_ids",
                "inactive_reasons",
            ):
                if isinstance(parsed.get(field), str) and str(parsed[field]).strip():
                    try:
                        parsed[field] = json.loads(str(parsed[field]))
                    except json.JSONDecodeError:
                        pass
            rows.append(parsed)
    return rows


def _assert_input_payload_safe(label: str, payload: Any) -> None:
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in BANNED_INPUT_TRUE_FIELDS and _bool(value):
            raise ScopeNarrowedCandidateRegenerationError(
                f"{label}: {'.'.join(path)} must be false"
            )
        if key == "broker_action" and str(value or "") != "none":
            raise ScopeNarrowedCandidateRegenerationError(
                f"{label}: {'.'.join(path)} must be none"
            )
        if isinstance(value, str) and value in BANNED_RECOMMENDATIONS:
            raise ScopeNarrowedCandidateRegenerationError(
                f"{label}: banned recommendation {value}"
            )


def _assert_generated_payload_safe(label: str, payload: Any) -> None:
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in BANNED_OUTPUT_TRUE_FIELDS and _bool(value):
            raise ScopeNarrowedCandidateRegenerationError(
                f"{label}: {'.'.join(path)} must be false"
            )
        if key == "broker_action" and str(value or "") != "none":
            raise ScopeNarrowedCandidateRegenerationError(
                f"{label}: {'.'.join(path)} must be none"
            )
        if isinstance(value, str) and value in BANNED_RECOMMENDATIONS:
            raise ScopeNarrowedCandidateRegenerationError(
                f"{label}: banned recommendation {value}"
            )


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


def _validator_record_sample(
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
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


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    parsed = tuple(str(item).strip() for item in value if str(item).strip())
    if not parsed:
        raise ScopeNarrowedCandidateRegenerationError("input list must be non-empty")
    return parsed


def _rows_from_payload(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    rows = payload.get(key)
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _single_candidate_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    refined_candidate_id: str,
    label: str,
) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("refined_candidate_id")) == refined_candidate_id
        or str(row.get("candidate_id")) == refined_candidate_id
    ]
    if len(matches) != 1:
        raise ScopeNarrowedCandidateRegenerationError(
            f"{label} requires exactly one row for {refined_candidate_id}, got {len(matches)}"
        )
    return matches[0]


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                return tuple(item.strip() for item in text.split(",") if item.strip())
        else:
            return tuple(item.strip() for item in text.split(",") if item.strip())
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _provenance(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = payload.get("provenance")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == []


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _walk_payload(value: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    rows: list[tuple[tuple[str, ...], Any]] = [(path, value)]
    if isinstance(value, Mapping):
        for key, item in value.items():
            rows.extend(_walk_payload(item, (*path, str(key))))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            rows.extend(_walk_payload(item, (*path, str(index))))
    return rows


def _csv_value(value: Any) -> Any:
    if isinstance(value, Mapping) or isinstance(value, list | tuple):
        return json.dumps(clean_for_yaml(value), ensure_ascii=False, sort_keys=True)
    return value


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _count_by(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts = Counter(str(row.get(field)) for row in rows)
    return dict(sorted(counts.items()))


def _dedupe_reasons(reasons: Sequence[str]) -> list[str]:
    seen = set()
    result = []
    for reason in INACTIVE_REASON_ORDER:
        if reason in reasons and reason not in seen:
            result.append(reason)
            seen.add(reason)
    for reason in reasons:
        if reason not in seen:
            result.append(reason)
            seen.add(reason)
    return result


def _primary_inactive_reason(reasons: Sequence[str]) -> str:
    ordered = _dedupe_reasons(reasons)
    return ordered[0] if ordered else "inactive_scope"


def _source_refined_record_id(row: Mapping[str, Any], row_index: int) -> str:
    record_id = row.get("record_id")
    if record_id:
        return str(record_id)
    return "|".join(
        [
            str(row.get("candidate_id") or row.get("refined_candidate_id") or ""),
            str(row.get("source_row_index") or row_index),
            str(row.get("target_asset") or ""),
            str(row.get("horizon") or ""),
            str(row.get("signal_name") or ""),
            str(row.get("as_of_timestamp") or ""),
        ]
    )


def _scope_numeric_fields(usage_role: str) -> tuple[str, ...]:
    if usage_role == "confirmation_only":
        return (
            "signal_value",
            "signal_confidence",
            "confirmation_score",
            "confirmation_confidence",
        )
    return ("signal_value", "signal_confidence", "risk_cap_score")


def _render_regeneration_report(
    run_summary: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> str:
    summary = _mapping(run_summary.get("summary"))
    rows = _rows_from_payload(registry, "rows")
    lines = [
        "# Scope-Narrowed Candidate Regeneration Report",
        "",
        (
            "TRADING-2291 只生成 scope-narrowed candidate-bound artifacts，"
            "不执行 actual-path validation。"
        ),
        "",
        f"- status: `{summary.get('status')}`",
        f"- included_candidate_count: `{summary.get('included_candidate_count')}`",
        f"- archived_candidate_count: `{summary.get('archived_candidate_count')}`",
        f"- next_task: `{summary.get('next_task')}`",
        "",
        "|scope_narrowed_candidate_id|refined_candidate_id|usage_role|active_record_count|inactive_record_count|",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "|{scope}|{refined}|{usage}|{active}|{inactive}|".format(
                scope=row.get("scope_narrowed_candidate_id"),
                refined=row.get("refined_candidate_id"),
                usage=row.get("usage_role"),
                active=row.get("active_record_count"),
                inactive=row.get("inactive_record_count"),
            )
        )
    lines.extend(
        [
            "",
            "所有输出固定 promotion / paper-shadow / production / broker action 为 false / none；",
            (
                "scope narrowing 不是 owner approval，也不是 paper-shadow、production "
                "或 broker readiness。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_scope_filter_doc(bundles: Sequence[ScopeNarrowedCandidateBundle]) -> str:
    lines = [
        "# Scope-Narrowed Candidate Scope Filter Report",
        "",
        "|scope_narrowed_candidate_id|usage_role|source_record_count|active_record_count|active_ratio|inactive_reasons|",
        "|---|---|---:|---:|---:|---|",
    ]
    for bundle in bundles:
        report = bundle.scope_filter_report
        lines.append(
            "|{cid}|{usage}|{source}|{active}|{ratio}|{reasons}|".format(
                cid=report.get("scope_narrowed_candidate_id"),
                usage=report.get("usage_role"),
                source=report.get("source_record_count"),
                active=report.get("active_record_count"),
                ratio=report.get("active_ratio"),
                reasons=json.dumps(report.get("inactive_reasons"), ensure_ascii=False),
            )
        )
    lines.extend(
        [
            "",
            "`baseline_plus_trend_structure` 被收窄为 `confirmation_only`；"
            "`volatility_regime` 被收窄为 `risk_cap_only`。",
            "Inactive records 保留在 artifacts 中，并通过 inactive reasons 显式记录过滤原因。",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_delta_doc(delta_summary: Mapping[str, Any]) -> str:
    rows = _rows_from_payload(delta_summary, "candidate_rows")
    lines = [
        "# Scope-Narrowed Original vs Refined Delta Summary",
        "",
        (
            "本报告只比较 distribution，不比较 forward return、alignment、"
            "false cost improvement 或 actual-path improvement。"
        ),
        "",
        "|scope_narrowed_candidate_id|usage_role|refined_records|scope_records|active_ratio|",
        "|---|---|---:|---:|---:|",
    ]
    for row in rows:
        refined = _mapping(row.get("refined_distribution"))
        scope = _mapping(row.get("scope_narrowed_distribution"))
        lines.append(
            "|{cid}|{usage}|{refined_count}|{scope_count}|{active_ratio}|".format(
                cid=row.get("scope_narrowed_candidate_id"),
                usage=row.get("usage_role"),
                refined_count=refined.get("record_count"),
                scope_count=scope.get("record_count"),
                active_ratio=scope.get("active_ratio"),
            )
        )
    return "\n".join(lines) + "\n"


def _render_risk_appetite_archive_doc(archive_record: Mapping[str, Any]) -> str:
    return (
        "# Risk Appetite Current Form Archive Record\n\n"
        f"- candidate_id: `{archive_record.get('candidate_id')}`\n"
        f"- archive_scope: `{archive_record.get('archive_scope')}`\n"
        f"- archive_reason: `{archive_record.get('archive_reason')}`\n"
        "- risk_appetite_regenerated: `false`\n"
        "- broker_action: `none`\n\n"
        "Archive current form 不代表永久否定 risk appetite concept；它只表示当前 "
        "generator + refined confidence scaling 路线不进入下一轮研究。\n"
    )
