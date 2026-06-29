from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from typing import Any

from ai_trading_system.candidate_signal_binding_schema import (
    PREDICTION_SCHEMA_VERSION,
    SIGNAL_SPEC_VERSION,
    CandidateArtifactProvenance,
    CandidateBoundPredictionArtifact,
    CandidateBoundSignalRecord,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateGeneratorContext,
    CandidateGeneratorError,
    CandidateSignalSpec,
    candidate_artifact_safety_fields,
    framework_smoke_artifact_safety_fields,
    generator_operation_safety_fields,
    trading_2281_boundary_fields,
)

FRAMEWORK_SMOKE_CANDIDATE_ID = "framework_smoke_candidate"
FRAMEWORK_SMOKE_CANDIDATE_FAMILY = "first_layer_executable_candidate"
FRAMEWORK_SMOKE_GENERATOR_VERSION = "framework_smoke_candidate_generator.v1"
FRAMEWORK_SMOKE_SIGNAL_NAME = "framework_smoke_signal"
FRAMEWORK_SMOKE_MODEL_OR_RULE_VERSION = "framework_smoke_rule.v1"
FRAMEWORK_SMOKE_ARTIFACT_ROLE = "framework_smoke_test"


class FrameworkSmokeCandidateGenerator:
    @property
    def generator_id(self) -> str:
        return FRAMEWORK_SMOKE_CANDIDATE_ID

    @property
    def generator_version(self) -> str:
        return FRAMEWORK_SMOKE_GENERATOR_VERSION

    @property
    def candidate_family(self) -> str:
        return FRAMEWORK_SMOKE_CANDIDATE_FAMILY

    def build_signal_spec(
        self,
        context: CandidateGeneratorContext,
    ) -> CandidateSignalSpec:
        _assert_framework_smoke_context(context)
        return CandidateSignalSpec(
            candidate_id=context.candidate_id,
            candidate_family=context.candidate_family,
            generator_id=self.generator_id,
            generator_version=self.generator_version,
            signal_spec_version=context.signal_spec_version,
            prediction_schema_version=context.prediction_schema_version,
            target_asset=context.target_asset,
            supported_horizons=(context.horizon,),
            required_inputs=("deterministic_framework_smoke_calendar",),
            output_signal_names=(FRAMEWORK_SMOKE_SIGNAL_NAME,),
            signal_direction_mapping={
                "negative": "risk_off",
                "zero": "neutral",
                "positive": "risk_on",
            },
            validity_rule="valid_until is valid_from plus parsed horizon calendar days",
            pit_policy="strict_pit",
            **generator_operation_safety_fields(),
        )

    def generate_signal_series(
        self,
        context: CandidateGeneratorContext,
        signal_spec: CandidateSignalSpec,
    ) -> list[CandidateBoundSignalRecord]:
        _assert_framework_smoke_context(context)
        horizon_days = _parse_horizon_days(context.horizon)
        source_path = context.source_paths[0]
        source_hash = context.source_hashes[0]
        provenance = _provenance(context)
        records: list[CandidateBoundSignalRecord] = []
        for index, current_date in enumerate(_business_dates(context), start=1):
            as_of = datetime.combine(current_date, time(21, 0), tzinfo=UTC)
            decision = as_of + timedelta(days=1)
            valid_from = decision
            valid_until = valid_from + timedelta(days=horizon_days)
            signal_value = _signal_value(index)
            signal_confidence = _signal_confidence(index)
            records.append(
                CandidateBoundSignalRecord(
                    candidate_id=context.candidate_id,
                    candidate_family=context.candidate_family,
                    source_experiment_id=signal_spec.generator_id,
                    source_artifact_id=source_path.stem,
                    source_artifact_path=str(source_path),
                    source_artifact_hash=source_hash,
                    signal_spec_version=SIGNAL_SPEC_VERSION,
                    prediction_schema_version=PREDICTION_SCHEMA_VERSION,
                    generated_at=context.generated_at.isoformat(),
                    as_of_timestamp=as_of.isoformat(),
                    decision_timestamp=decision.isoformat(),
                    target_asset=context.target_asset,
                    horizon=context.horizon,
                    signal_name=FRAMEWORK_SMOKE_SIGNAL_NAME,
                    signal_value=signal_value,
                    signal_direction=_signal_direction(signal_value),
                    signal_confidence=signal_confidence,
                    valid_from=valid_from.isoformat(),
                    valid_until=valid_until.isoformat(),
                    input_snapshot_hash=context.input_snapshot_hash,
                    feature_snapshot_hash=context.feature_snapshot_hash,
                    model_or_rule_version=FRAMEWORK_SMOKE_MODEL_OR_RULE_VERSION,
                    provenance=provenance,
                    **candidate_artifact_safety_fields(),
                    source_row_index=index,
                    source_date=current_date.isoformat(),
                    source_trend_state="framework_smoke_test",
                    source_confidence=signal_confidence,
                    source_prediction_flags={
                        "framework_smoke_test": True,
                        "actual_path_validation_ready": False,
                    },
                )
            )
        if not records:
            raise CandidateGeneratorError("framework smoke candidate generated no records")
        return records

    def generate_prediction_artifact(
        self,
        context: CandidateGeneratorContext,
        signal_spec: CandidateSignalSpec,
        signal_records: list[CandidateBoundSignalRecord],
    ) -> dict[str, Any]:
        if not signal_records:
            raise CandidateGeneratorError("prediction artifact requires signal records")
        latest = signal_records[-1]
        source_path = context.source_paths[0]
        source_hash = context.source_hashes[0]
        prediction_records = [_prediction_record(record) for record in signal_records]
        artifact = CandidateBoundPredictionArtifact(
            artifact_id=f"{context.candidate_id}_prediction_artifact",
            artifact_role=FRAMEWORK_SMOKE_ARTIFACT_ROLE,
            candidate_id=context.candidate_id,
            candidate_family=context.candidate_family,
            source_experiment_id=signal_spec.generator_id,
            source_artifact_id=source_path.stem,
            source_artifact_path=str(source_path),
            source_artifact_hash=source_hash,
            signal_spec_version=context.signal_spec_version,
            prediction_schema_version=context.prediction_schema_version,
            generated_at=context.generated_at.isoformat(),
            as_of_timestamp=latest.as_of_timestamp,
            decision_timestamp=latest.decision_timestamp,
            target_asset=context.target_asset,
            horizon=context.horizon,
            signal_name=FRAMEWORK_SMOKE_SIGNAL_NAME,
            signal_value=latest.signal_value,
            signal_direction=latest.signal_direction,
            signal_confidence=latest.signal_confidence,
            valid_from=latest.valid_from,
            valid_until=latest.valid_until,
            input_snapshot_hash=context.input_snapshot_hash,
            feature_snapshot_hash=context.feature_snapshot_hash,
            model_or_rule_version=FRAMEWORK_SMOKE_MODEL_OR_RULE_VERSION,
            provenance=_provenance(context),
            prediction_records=prediction_records,
            source_schema_status="candidate_bound",
            **framework_smoke_artifact_safety_fields(),
        )
        payload = artifact.to_dict()
        payload["schema_version"] = context.prediction_schema_version
        payload["record_count"] = len(prediction_records)
        payload["generation_mode"] = context.mode
        payload["candidate_binding_method"] = "native_candidate_id"
        payload["framework_smoke_candidate_validation_only"] = True
        payload.update(trading_2281_boundary_fields())
        return payload


def _assert_framework_smoke_context(context: CandidateGeneratorContext) -> None:
    if context.candidate_id != FRAMEWORK_SMOKE_CANDIDATE_ID:
        raise CandidateGeneratorError(
            "framework smoke generator only supports framework_smoke_candidate"
        )
    if context.candidate_family != FRAMEWORK_SMOKE_CANDIDATE_FAMILY:
        raise CandidateGeneratorError(
            "framework smoke generator requires first_layer_executable_candidate family"
        )
    if context.mode != FRAMEWORK_SMOKE_ARTIFACT_ROLE:
        raise CandidateGeneratorError(
            "framework smoke generator only supports framework_smoke_test"
        )


def _provenance(context: CandidateGeneratorContext) -> CandidateArtifactProvenance:
    return CandidateArtifactProvenance(
        source_paths=[str(path) for path in context.source_paths],
        source_hashes=list(context.source_hashes),
        regeneration_mode=FRAMEWORK_SMOKE_ARTIFACT_ROLE,
        pit_policy="strict_pit",
        candidate_binding_method="native_candidate_id",
        source_schema_status="candidate_bound",
        promotion_eligible=candidate_artifact_safety_fields()["promotion_eligible"],
    )


def _prediction_record(record: CandidateBoundSignalRecord) -> dict[str, Any]:
    payload = record.to_dict()
    payload["prediction_fields"] = {
        "candidate_signal": record.signal_name,
        "signal_value": record.signal_value,
        "signal_direction": record.signal_direction,
        "signal_confidence": record.signal_confidence,
        "framework_smoke_test": True,
    }
    return payload


def _business_dates(context: CandidateGeneratorContext) -> list[Any]:
    current = context.start_date
    output = []
    while current <= context.end_date:
        if current.weekday() < 5:
            output.append(current)
        current = current + timedelta(days=1)
    return output


def _parse_horizon_days(horizon: str) -> int:
    text = horizon.strip().lower()
    if not text.endswith("d"):
        raise CandidateGeneratorError("horizon must use day suffix, e.g. 10d")
    try:
        days = int(text[:-1])
    except ValueError as exc:
        raise CandidateGeneratorError("horizon day count must be an integer") from exc
    if days <= 0:
        raise CandidateGeneratorError("horizon day count must be positive")
    return days


def _signal_value(index: int) -> float:
    return round(((index - 1) % 5 - 2) / 2.0, 6)


def _signal_confidence(index: int) -> float:
    return round(0.4 + (((index - 1) % 5) * 0.1), 6)


def _signal_direction(signal_value: float) -> str:
    if signal_value > 0:
        return "risk_on"
    if signal_value < 0:
        return "risk_off"
    return "neutral"
