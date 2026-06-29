from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

from ai_trading_system.candidate_signal_binding_schema import (
    CandidateBoundPredictionArtifact,
    CandidateBoundSignalRecord,
)
from ai_trading_system.post_2085_research_common import clean_for_yaml

ALLOWED_CANDIDATE_GENERATOR_MODES = ("framework_smoke_test",)


class CandidateGeneratorError(ValueError):
    """Raised when candidate generation cannot produce a validated fail-closed bundle."""


# Invariant research-only safety metadata for candidate generator framework outputs.
# These are not tunable thresholds or investment policy knobs.
def generator_operation_safety_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def candidate_artifact_safety_fields() -> dict[str, Any]:
    return {
        "promotion_eligible": False,
        **generator_operation_safety_fields(),
        "permanently_inconclusive_override_allowed": False,
    }


def framework_smoke_artifact_safety_fields() -> dict[str, Any]:
    return {
        "historical_executable_artifact": False,
        "actual_path_validation_ready": False,
        **candidate_artifact_safety_fields(),
    }


def trading_2281_boundary_fields() -> dict[str, Any]:
    return {
        "trading_2281_permanently_inconclusive_decisions_changed": False,
    }


@dataclass(frozen=True)
class CandidateGeneratorContext:
    candidate_id: str
    candidate_family: str
    target_asset: str
    start_date: date
    end_date: date
    horizon: str
    output_dir: Path
    mode: str
    generated_at: datetime
    signal_spec_version: str
    prediction_schema_version: str
    input_snapshot_hash: str
    feature_snapshot_hash: str
    source_paths: tuple[Path, ...]
    source_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "candidate_id",
            "candidate_family",
            "target_asset",
            "horizon",
            "mode",
            "signal_spec_version",
            "prediction_schema_version",
            "input_snapshot_hash",
            "feature_snapshot_hash",
        ):
            if not str(getattr(self, field_name)).strip():
                raise CandidateGeneratorError(f"missing {field_name}")
        if self.mode not in ALLOWED_CANDIDATE_GENERATOR_MODES:
            raise CandidateGeneratorError(f"unsupported candidate generator mode: {self.mode}")
        if self.start_date > self.end_date:
            raise CandidateGeneratorError("start_date must be <= end_date")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise CandidateGeneratorError("generated_at must be timezone-aware")
        if not self.source_paths:
            raise CandidateGeneratorError("source_paths must be non-empty")
        if not self.source_hashes:
            raise CandidateGeneratorError("source_hashes must be non-empty")
        if len(self.source_paths) != len(self.source_hashes):
            raise CandidateGeneratorError("source_paths and source_hashes length mismatch")
        if any(not str(item).strip() for item in self.source_hashes):
            raise CandidateGeneratorError("source_hashes must be non-empty")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        payload["generated_at"] = self.generated_at.isoformat()
        payload["output_dir"] = str(self.output_dir)
        payload["source_paths"] = [str(path) for path in self.source_paths]
        payload["source_hashes"] = list(self.source_hashes)
        return clean_for_yaml(payload)


@dataclass(frozen=True)
class CandidateSignalSpec:
    candidate_id: str
    candidate_family: str
    generator_id: str
    generator_version: str
    signal_spec_version: str
    prediction_schema_version: str
    target_asset: str
    supported_horizons: tuple[str, ...]
    required_inputs: tuple[str, ...]
    output_signal_names: tuple[str, ...]
    signal_direction_mapping: dict[str, str]
    validity_rule: str
    pit_policy: str
    promotion_allowed: bool
    paper_shadow_allowed: bool
    production_allowed: bool
    broker_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema_version"] = self.signal_spec_version
        payload["artifact_role"] = "candidate_signal_spec"
        payload["supported_horizons"] = list(self.supported_horizons)
        payload["required_inputs"] = list(self.required_inputs)
        payload["output_signal_names"] = list(self.output_signal_names)
        payload.update(candidate_artifact_safety_fields())
        return clean_for_yaml(payload)


@dataclass(frozen=True)
class CandidateGenerationBundle:
    context: CandidateGeneratorContext
    signal_spec: CandidateSignalSpec
    signal_records: list[CandidateBoundSignalRecord]
    prediction_artifact: dict[str, Any]

    def signal_payloads(self) -> list[dict[str, Any]]:
        return [record.to_dict() for record in self.signal_records]


@dataclass(frozen=True)
class CandidateGeneratorResult:
    status: str
    candidate_id: str
    artifact_paths: dict[str, str]
    generation_summary: dict[str, Any]
    validation_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return clean_for_yaml(asdict(self))


class FirstLayerCandidateSignalGenerator(Protocol):
    @property
    def generator_id(self) -> str: ...

    @property
    def generator_version(self) -> str: ...

    @property
    def candidate_family(self) -> str: ...

    def build_signal_spec(
        self,
        context: CandidateGeneratorContext,
    ) -> CandidateSignalSpec: ...

    def generate_signal_series(
        self,
        context: CandidateGeneratorContext,
        signal_spec: CandidateSignalSpec,
    ) -> list[CandidateBoundSignalRecord]: ...

    def generate_prediction_artifact(
        self,
        context: CandidateGeneratorContext,
        signal_spec: CandidateSignalSpec,
        signal_records: list[CandidateBoundSignalRecord],
    ) -> CandidateBoundPredictionArtifact | dict[str, Any]: ...
