from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

SCHEMA_VERSION = "candidate_signal_binding_schema.v1"
SIGNAL_SERIES_CONTRACT_VERSION = "candidate_bound_signal_series_contract.v1"
PREDICTION_ARTIFACT_CONTRACT_VERSION = "candidate_bound_prediction_artifact_contract.v1"
SIGNAL_SPEC_VERSION = "first_layer_candidate_signal_spec.v1"
PREDICTION_SCHEMA_VERSION = "candidate_bound_prediction_artifact.v1"

ALLOWED_SIGNAL_DIRECTIONS = (
    "risk_on",
    "risk_off",
    "neutral",
    "trend_confirming",
    "trend_weakening",
    "volatility_expansion",
    "volatility_compression",
)
ALLOWED_REGENERATION_MODES = (
    "original_generation",
    "deterministic_regeneration",
    "framework_smoke_test",
    "schema_migration_poc",
    "manual_review_attachment",
)
ALLOWED_PIT_POLICIES = (
    "strict_pit",
    "pit_approximation",
    "non_pit_source_evidence_only",
)
ALLOWED_CANDIDATE_BINDING_METHODS = (
    "native_candidate_id",
    "registry_binding",
    "rewrap_mapping",
    "manual_owner_mapping",
)
ALLOWED_SOURCE_SCHEMA_STATUSES = (
    "candidate_bound",
    "source_evidence_only",
    "schema_incompatible",
    "missing_candidate_id",
)

REQUIRED_SIGNAL_FIELDS = (
    "candidate_id",
    "candidate_family",
    "source_experiment_id",
    "source_artifact_id",
    "source_artifact_path",
    "source_artifact_hash",
    "signal_spec_version",
    "prediction_schema_version",
    "generated_at",
    "as_of_timestamp",
    "decision_timestamp",
    "target_asset",
    "horizon",
    "signal_name",
    "signal_value",
    "signal_direction",
    "signal_confidence",
    "valid_from",
    "valid_until",
    "input_snapshot_hash",
    "feature_snapshot_hash",
    "model_or_rule_version",
    "provenance",
)
REQUIRED_PROVENANCE_FIELDS = (
    "source_paths",
    "source_hashes",
    "regeneration_mode",
    "pit_policy",
    "candidate_binding_method",
    "source_schema_status",
    "promotion_eligible",
)
REQUIRED_SAFETY_FIELDS = (
    "promotion_eligible",
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "broker_action",
    "permanently_inconclusive_override_allowed",
)


@dataclass(frozen=True)
class CandidateArtifactProvenance:
    source_paths: list[str]
    source_hashes: list[str]
    regeneration_mode: str
    pit_policy: str
    candidate_binding_method: str
    source_schema_status: str
    promotion_eligible: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateBoundSignalRecord:
    candidate_id: str
    candidate_family: str
    source_experiment_id: str
    source_artifact_id: str
    source_artifact_path: str
    source_artifact_hash: str
    signal_spec_version: str
    prediction_schema_version: str
    generated_at: str
    as_of_timestamp: str
    decision_timestamp: str
    target_asset: str
    horizon: str
    signal_name: str
    signal_value: float
    signal_direction: str
    signal_confidence: float
    valid_from: str
    valid_until: str
    input_snapshot_hash: str
    feature_snapshot_hash: str
    model_or_rule_version: str
    provenance: CandidateArtifactProvenance
    promotion_eligible: bool
    promotion_allowed: bool
    paper_shadow_allowed: bool
    production_allowed: bool
    broker_action: str
    permanently_inconclusive_override_allowed: bool
    source_row_index: int
    source_date: str
    source_trend_state: str
    source_confidence: float
    source_prediction_flags: dict[str, bool]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["provenance"] = self.provenance.to_dict()
        return payload


@dataclass(frozen=True)
class CandidateBoundPredictionArtifact:
    artifact_id: str
    artifact_role: str
    candidate_id: str
    candidate_family: str
    source_experiment_id: str
    source_artifact_id: str
    source_artifact_path: str
    source_artifact_hash: str
    signal_spec_version: str
    prediction_schema_version: str
    generated_at: str
    as_of_timestamp: str
    decision_timestamp: str
    target_asset: str
    horizon: str
    signal_name: str
    signal_value: float
    signal_direction: str
    signal_confidence: float
    valid_from: str
    valid_until: str
    input_snapshot_hash: str
    feature_snapshot_hash: str
    model_or_rule_version: str
    provenance: CandidateArtifactProvenance
    prediction_records: list[dict[str, Any]]
    source_schema_status: str
    historical_executable_artifact: bool
    actual_path_validation_ready: bool
    promotion_eligible: bool
    promotion_allowed: bool
    paper_shadow_allowed: bool
    production_allowed: bool
    broker_action: str
    permanently_inconclusive_override_allowed: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["provenance"] = self.provenance.to_dict()
        return payload


@dataclass(frozen=True)
class CandidateSignalBindingValidationResult:
    passed: bool
    checked_record_count: int
    errors: list[str]
    warnings: list[str]

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "checked_record_count": self.checked_record_count,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class CandidateSignalBindingSchema:
    schema_version: str = SCHEMA_VERSION
    required_fields: tuple[str, ...] = REQUIRED_SIGNAL_FIELDS
    required_provenance_fields: tuple[str, ...] = REQUIRED_PROVENANCE_FIELDS
    required_safety_fields: tuple[str, ...] = REQUIRED_SAFETY_FIELDS

    def to_dict(self) -> dict[str, Any]:
        return candidate_signal_binding_schema_dict()


def candidate_signal_binding_schema_dict() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pilot_baseline",
        "artifact_role": "schema_contract",
        "required_fields": list(REQUIRED_SIGNAL_FIELDS),
        "fields": {
            "candidate_id": {"type": "string", "required": True},
            "candidate_family": {"type": "string", "required": True},
            "source_experiment_id": {"type": "string", "required": True},
            "source_artifact_id": {"type": "string", "required": True},
            "source_artifact_path": {"type": "string", "required": True},
            "source_artifact_hash": {"type": "string", "required": True},
            "signal_spec_version": {"type": "string", "required": True},
            "prediction_schema_version": {"type": "string", "required": True},
            "generated_at": {"type": "datetime", "required": True},
            "as_of_timestamp": {"type": "datetime", "required": True},
            "decision_timestamp": {"type": "datetime", "required": True},
            "target_asset": {"type": "string", "required": True},
            "horizon": {"type": "string", "required": True},
            "signal_name": {"type": "string", "required": True},
            "signal_value": {"type": "number", "required": True},
            "signal_direction": {
                "type": "enum",
                "values": list(ALLOWED_SIGNAL_DIRECTIONS),
                "required": True,
            },
            "signal_confidence": {"type": "number", "min": 0.0, "max": 1.0, "required": True},
            "valid_from": {"type": "datetime", "required": True},
            "valid_until": {"type": "datetime", "required": True},
            "input_snapshot_hash": {"type": "string", "required": True},
            "feature_snapshot_hash": {"type": "string", "required": True},
            "model_or_rule_version": {"type": "string", "required": True},
            "provenance": {"type": "object", "required": True},
        },
        "provenance": {
            "required_fields": list(REQUIRED_PROVENANCE_FIELDS),
            "fields": {
                "source_paths": {"type": "list[string]", "required": True},
                "source_hashes": {"type": "list[string]", "required": True},
                "regeneration_mode": {
                    "type": "enum",
                    "values": list(ALLOWED_REGENERATION_MODES),
                    "required": True,
                },
                "pit_policy": {
                    "type": "enum",
                    "values": list(ALLOWED_PIT_POLICIES),
                    "required": True,
                },
                "candidate_binding_method": {
                    "type": "enum",
                    "values": list(ALLOWED_CANDIDATE_BINDING_METHODS),
                    "required": True,
                },
                "source_schema_status": {
                    "type": "enum",
                    "values": list(ALLOWED_SOURCE_SCHEMA_STATUSES),
                    "required": True,
                },
                "promotion_eligible": {"type": "boolean", "required": True},
            },
        },
        "safety_boundary": {
            "schema_migration_poc_requires_promotion_eligible_false": True,
            "non_pit_source_evidence_only_requires_paper_shadow_allowed_false": True,
            "non_pit_source_evidence_only_requires_production_allowed_false": True,
            "non_pit_source_evidence_only_requires_broker_action_none": True,
            "permanently_inconclusive_override_allowed": False,
        },
    }


def candidate_bound_signal_series_contract_dict() -> dict[str, Any]:
    return {
        "schema_version": SIGNAL_SERIES_CONTRACT_VERSION,
        "artifact_type": "candidate_bound_signal_series",
        "file_format": "csv",
        "required_columns": list(REQUIRED_SIGNAL_FIELDS)
        + list(REQUIRED_SAFETY_FIELDS)
        + [
            "source_row_index",
            "source_date",
            "source_trend_state",
            "source_confidence",
            "source_prediction_flags",
        ],
        "provenance_column_encoding": "json_object_string",
        "validation_rules": _validation_rule_descriptions(),
        "schema_versioning_policy": "additive_changes_only_until_v2_owner_review",
        "backward_compatibility_policy": (
            "v1 readers must fail closed when required candidate binding, PIT, hash, "
            "or provenance fields are absent"
        ),
        "failure_examples": [
            "missing candidate_id",
            "missing as_of_timestamp",
            "valid_until earlier than valid_from",
            "schema_migration_poc with promotion_eligible=true",
            "framework_smoke_test marked actual_path_validation_ready=true",
            "non_pit_source_evidence_only with paper_shadow_allowed=true",
        ],
    }


def candidate_bound_prediction_artifact_contract_dict() -> dict[str, Any]:
    return {
        "schema_version": PREDICTION_ARTIFACT_CONTRACT_VERSION,
        "artifact_type": "candidate_bound_prediction_artifact",
        "file_format": "json",
        "required_top_level_fields": [
            "artifact_id",
            "artifact_role",
            *REQUIRED_SIGNAL_FIELDS,
            *REQUIRED_SAFETY_FIELDS,
            "prediction_records",
            "historical_executable_artifact",
            "actual_path_validation_ready",
        ],
        "prediction_record_contract": (
            "Each prediction record must preserve candidate_id, timestamps, horizon, "
            "source hash, schema versions, provenance, and safety fields."
        ),
        "validation_rules": _validation_rule_descriptions(),
        "schema_versioning_policy": "v1 is append-only; breaking changes require v2 contract",
        "backward_compatibility_policy": (
            "missing required binding, PIT, hash, provenance, or safety fields fails closed"
        ),
        "failure_examples": [
            "prediction artifact lacks source_artifact_hash",
            "prediction artifact lacks provenance.regeneration_mode",
            "schema_migration_poc marked promotion_eligible=true",
            "framework_smoke_test marked historical_executable_artifact=true",
            "non_pit_source_evidence_only marked production_allowed=true",
            "broker_action other than none",
        ],
    }


def _validation_rule_descriptions() -> list[str]:
    return [
        "candidate_id exists and is non-empty",
        "as_of_timestamp and decision_timestamp exist",
        "valid_from and valid_until exist and valid_until >= valid_from",
        "horizon, signal_spec_version, and prediction_schema_version exist",
        "input_snapshot_hash, feature_snapshot_hash, and source_artifact_hash exist",
        "provenance.regeneration_mode, pit_policy, and candidate_binding_method exist",
        "promotion_eligible exists and schema_migration_poc forces it to false",
        "non_pit_source_evidence_only forces paper_shadow_allowed=false",
        "non_pit_source_evidence_only forces production_allowed=false",
        "non_pit_source_evidence_only forces broker_action=none",
    ]
