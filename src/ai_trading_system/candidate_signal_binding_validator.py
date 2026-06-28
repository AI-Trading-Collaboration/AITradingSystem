from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from ai_trading_system.candidate_signal_binding_schema import (
    ALLOWED_CANDIDATE_BINDING_METHODS,
    ALLOWED_PIT_POLICIES,
    ALLOWED_REGENERATION_MODES,
    ALLOWED_SIGNAL_DIRECTIONS,
    ALLOWED_SOURCE_SCHEMA_STATUSES,
    REQUIRED_PROVENANCE_FIELDS,
    REQUIRED_SAFETY_FIELDS,
    REQUIRED_SIGNAL_FIELDS,
    CandidateSignalBindingValidationResult,
)


class CandidateSignalBindingValidator:
    def validate_candidate_bound_signal_series(
        self,
        records: Sequence[Mapping[str, Any]],
    ) -> CandidateSignalBindingValidationResult:
        errors: list[str] = []
        warnings: list[str] = []
        if not records:
            errors.append("signal_series: no records")
        for index, record in enumerate(records):
            errors.extend(self._validate_common_record(record, scope=f"signal_series[{index}]"))
        return CandidateSignalBindingValidationResult(
            passed=not errors,
            checked_record_count=len(records),
            errors=errors,
            warnings=warnings,
        )

    def validate_candidate_bound_prediction_artifact(
        self,
        artifact: Mapping[str, Any],
    ) -> CandidateSignalBindingValidationResult:
        errors = self._validate_common_record(artifact, scope="prediction_artifact")
        records = artifact.get("prediction_records")
        if not isinstance(records, list) or not records:
            errors.append("prediction_artifact: prediction_records missing or empty")
            checked_record_count = 0
        else:
            checked_record_count = len(records)
            for index, record in enumerate(records):
                if isinstance(record, Mapping):
                    errors.extend(
                        self._validate_common_record(
                            record,
                            scope=f"prediction_artifact.prediction_records[{index}]",
                        )
                    )
                else:
                    errors.append(
                        f"prediction_artifact.prediction_records[{index}]: record is not object"
                    )
        for field in ("artifact_id", "artifact_role", "historical_executable_artifact"):
            if _is_missing(artifact.get(field)):
                errors.append(f"prediction_artifact: missing {field}")
        if artifact.get("artifact_role") == "schema_migration_poc":
            if artifact.get("historical_executable_artifact") is not False:
                errors.append(
                    "prediction_artifact: schema_migration_poc must not be historical executable"
                )
            if artifact.get("actual_path_validation_ready") is not False:
                errors.append(
                    "prediction_artifact: schema_migration_poc must not be actual-path ready"
                )
        return CandidateSignalBindingValidationResult(
            passed=not errors,
            checked_record_count=checked_record_count,
            errors=errors,
            warnings=[],
        )

    def validate_promotion_gating(
        self,
        payload: Mapping[str, Any],
    ) -> CandidateSignalBindingValidationResult:
        errors = self._validate_gating(payload, scope="promotion_gating")
        return CandidateSignalBindingValidationResult(
            passed=not errors,
            checked_record_count=1,
            errors=errors,
            warnings=[],
        )

    def _validate_common_record(self, record: Mapping[str, Any], *, scope: str) -> list[str]:
        errors: list[str] = []
        for field in REQUIRED_SIGNAL_FIELDS:
            if _is_missing(record.get(field)):
                errors.append(f"{scope}: missing {field}")
        for field in REQUIRED_SAFETY_FIELDS:
            if _is_missing(record.get(field)):
                errors.append(f"{scope}: missing {field}")
        if _is_missing(record.get("candidate_id")):
            errors.append(f"{scope}: candidate_id empty")

        signal_direction = str(record.get("signal_direction") or "")
        if signal_direction and signal_direction not in ALLOWED_SIGNAL_DIRECTIONS:
            errors.append(f"{scope}: unsupported signal_direction={signal_direction}")

        confidence = _to_float(record.get("signal_confidence"))
        if confidence is None:
            errors.append(f"{scope}: signal_confidence is not numeric")
        elif not 0.0 <= confidence <= 1.0:
            errors.append(f"{scope}: signal_confidence out of [0, 1]")

        valid_from = _parse_datetime(record.get("valid_from"))
        valid_until = _parse_datetime(record.get("valid_until"))
        if record.get("valid_from") and valid_from is None:
            errors.append(f"{scope}: valid_from is not datetime")
        if record.get("valid_until") and valid_until is None:
            errors.append(f"{scope}: valid_until is not datetime")
        if valid_from is not None and valid_until is not None and valid_until < valid_from:
            errors.append(f"{scope}: valid_until earlier than valid_from")

        provenance = _provenance(record)
        if not provenance:
            errors.append(f"{scope}: provenance is not object")
        else:
            errors.extend(self._validate_provenance(provenance, scope=scope))
        errors.extend(self._validate_gating(record, scope=scope))
        return errors

    def _validate_provenance(
        self,
        provenance: Mapping[str, Any],
        *,
        scope: str,
    ) -> list[str]:
        errors: list[str] = []
        for field in REQUIRED_PROVENANCE_FIELDS:
            if _is_missing(provenance.get(field)):
                errors.append(f"{scope}: missing provenance.{field}")
        if _as_list(provenance.get("source_paths")) == []:
            errors.append(f"{scope}: provenance.source_paths empty")
        if _as_list(provenance.get("source_hashes")) == []:
            errors.append(f"{scope}: provenance.source_hashes empty")
        _validate_enum(
            provenance,
            field="regeneration_mode",
            allowed=ALLOWED_REGENERATION_MODES,
            scope=scope,
            errors=errors,
        )
        _validate_enum(
            provenance,
            field="pit_policy",
            allowed=ALLOWED_PIT_POLICIES,
            scope=scope,
            errors=errors,
        )
        _validate_enum(
            provenance,
            field="candidate_binding_method",
            allowed=ALLOWED_CANDIDATE_BINDING_METHODS,
            scope=scope,
            errors=errors,
        )
        _validate_enum(
            provenance,
            field="source_schema_status",
            allowed=ALLOWED_SOURCE_SCHEMA_STATUSES,
            scope=scope,
            errors=errors,
        )
        return errors

    def _validate_gating(self, record: Mapping[str, Any], *, scope: str) -> list[str]:
        errors: list[str] = []
        provenance = _provenance(record)
        regeneration_mode = str(provenance.get("regeneration_mode") or "")
        pit_policy = str(provenance.get("pit_policy") or "")
        promotion_eligible = _to_bool(record.get("promotion_eligible"))
        provenance_promotion_eligible = _to_bool(provenance.get("promotion_eligible"))

        if regeneration_mode == "schema_migration_poc":
            if promotion_eligible is not False:
                errors.append(f"{scope}: schema_migration_poc requires promotion_eligible=false")
            if provenance_promotion_eligible is not False:
                errors.append(
                    f"{scope}: schema_migration_poc requires provenance.promotion_eligible=false"
                )
        if pit_policy == "non_pit_source_evidence_only":
            if _to_bool(record.get("paper_shadow_allowed")) is not False:
                errors.append(
                    f"{scope}: non_pit_source_evidence_only requires paper_shadow_allowed=false"
                )
            if _to_bool(record.get("production_allowed")) is not False:
                errors.append(
                    f"{scope}: non_pit_source_evidence_only requires production_allowed=false"
                )
            if str(record.get("broker_action") or "") != "none":
                errors.append(
                    f"{scope}: non_pit_source_evidence_only requires broker_action=none"
                )
        if _to_bool(record.get("promotion_allowed")) is not False:
            errors.append(f"{scope}: promotion_allowed must be false")
        if _to_bool(record.get("permanently_inconclusive_override_allowed")) is not False:
            errors.append(
                f"{scope}: permanently_inconclusive_override_allowed must be false"
            )
        return errors


def validate_candidate_bound_signal_series(
    records: Sequence[Mapping[str, Any]],
) -> CandidateSignalBindingValidationResult:
    return CandidateSignalBindingValidator().validate_candidate_bound_signal_series(records)


def validate_candidate_bound_prediction_artifact(
    artifact: Mapping[str, Any],
) -> CandidateSignalBindingValidationResult:
    return CandidateSignalBindingValidator().validate_candidate_bound_prediction_artifact(artifact)


def validate_promotion_gating(
    payload: Mapping[str, Any],
) -> CandidateSignalBindingValidationResult:
    return CandidateSignalBindingValidator().validate_promotion_gating(payload)


def _provenance(record: Mapping[str, Any]) -> dict[str, Any]:
    raw = record.get("provenance")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _parse_datetime(value: Any) -> datetime | None:
    if _is_missing(value):
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _validate_enum(
    payload: Mapping[str, Any],
    *,
    field: str,
    allowed: Sequence[str],
    scope: str,
    errors: list[str],
) -> None:
    value = str(payload.get(field) or "")
    if value and value not in set(allowed):
        errors.append(f"{scope}: unsupported provenance.{field}={value}")
