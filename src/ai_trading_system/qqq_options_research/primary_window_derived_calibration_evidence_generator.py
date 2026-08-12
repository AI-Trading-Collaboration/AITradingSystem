from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_text_atomic
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQReportRecord,
)
from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    OwnerDecisionCanonicalGroup,
    OwnerDecisionEvidenceClass,
)
from ai_trading_system.qqq_options_research.primary_window_policy_calibration import (
    CalibrationAggregateStatistic,
    CalibrationEvidenceReference,
    QQQOptionsPrimaryWindowCalibrationEvaluation,
    QQQOptionsPrimaryWindowCalibrationEvidenceRecord,
    build_qqq_options_primary_window_calibration_evaluation,
    load_qqq_options_primary_window_calibration_policy,
    resolve_qqq_options_primary_window_calibration_evaluation,
)
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_GENERATOR_POLICY_PATH = Path(
    "config/research/qqq_options_primary_window_derived_calibration_evidence_generator_v1.yaml"
)

_PRIMARY_RESEARCH_START = date(2021, 2, 22)
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_CANONICAL_DECIMAL_PATTERN = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")

_EXPECTED_SLOT_IDS = (
    "ACC_CASH_RESERVATION",
    "ACC_DQ_PIT_REPRO",
    "ACC_FEE_SCHEDULE",
    "ACC_RESULT_INCLUSION",
    "ACC_SAMPLE_COVERAGE",
    "ACC_SIZING_EXPOSURE",
    "EXE_MARKETABLE_LIMIT",
    "EXE_QUOTE_DISPOSITION",
    "LIFE_EXPIRY_EXIT_GUARD",
    "LIFE_TERMINAL_VALUATION",
    "SEL_DELTA_SOURCE_RANGE",
    "SEL_DTE_WINDOW",
    "SEL_MONEYNESS_RANGE",
    "SEL_OPEN_INTEREST_FLOOR",
    "SEL_QUOTE_FRESHNESS",
    "SEL_RANK_PRIORITY",
    "SEL_SPREAD_LIMIT",
    "SEL_VOLUME_FLOOR",
)


class QQQOptionsDerivedCalibrationEvidenceGeneratorError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class DerivedStatisticOperation(StrEnum):
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, use_enum_values=False)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a portable identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase 40-character Git SHA")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field} must use a timezone-aware UTC value")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant is prohibited: {value}")

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not canonical UTF-8 JSON") from exc


def _require_relative_path(value: str, field: str) -> str:
    checked = _required_text(value, field)
    path = Path(checked)
    if path.is_absolute() or path.drive or ".." in path.parts or path.as_posix() != checked:
        raise ValueError(f"{field} must be a normalized project-relative path")
    return checked


def _bound_path(path: Path, *, root: Path, field: str, must_exist: bool) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the configured root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the configured root")
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    if must_exist and not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    return candidate


def _trading_sessions(start: date, end: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while current <= end:
        if us_equity_market_session(current).is_trading_day:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _decimal(value: str, field: str) -> Decimal:
    if not _CANONICAL_DECIMAL_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a normalized finite decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{field} must be a normalized finite decimal string") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed


def _format_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_sha256(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _canonical_sha256(self.semantic_payload())

    @model_validator(mode="after")
    def _validate_content_seal(self, info: ValidationInfo) -> Self:
        allow_unsealed = bool(info.context and info.context.get("generator_allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("semantic content SHA-256 mismatch")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                "GENERATOR_PAYLOAD_MISMATCH",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        try:
            provisional = cls.model_validate(
                {**payload, "content_sha256": _UNSEALED_SHA256},
                context={"generator_allow_unsealed": True},
            )
            return cls.model_validate(
                {**payload, "content_sha256": provisional.compute_content_sha256()}
            )
        except (TypeError, ValueError) as exc:
            raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                "GENERATOR_PAYLOAD_MISMATCH", str(exc)
            ) from exc

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = _duplicate_key_rejecting_json(raw)
            if not isinstance(decoded, dict):
                raise ValueError("record JSON root must be an object")
            record = cls.model_validate_json(raw)
        except (TypeError, ValueError) as exc:
            raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                "GENERATOR_RECORD_INVALID", str(exc)
            ) from exc
        if record.canonical_bytes != raw:
            raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                "GENERATOR_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical UTF-8/LF JSON",
            )
        return record


class GeneratorStatisticDefinition(_PolicyModel):
    statistic_id: str
    unit_id: str
    operation: DerivedStatisticOperation

    @field_validator("statistic_id", "unit_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class GeneratorMetricDefinition(_PolicyModel):
    slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    evidence_class: OwnerDecisionEvidenceClass
    metric_definition_id: str
    metric_definition_version: Literal["1.0.0"]
    statistics: tuple[GeneratorStatisticDefinition, ...]

    @field_validator("slot_id", "metric_definition_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_statistics(self) -> Self:
        ids = tuple(item.statistic_id for item in self.statistics)
        if not ids or ids != tuple(sorted(ids)) or len(ids) != len(set(ids)):
            raise ValueError("metric statistics must be non-empty, unique, and sorted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


class GeneratorUpstreamCalibration(_PolicyModel):
    policy_path: str
    policy_file_sha256: str
    policy_canonical_sha256: str

    @field_validator("policy_path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        return _require_relative_path(value, "policy_path")

    @field_validator("policy_file_sha256", "policy_canonical_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class GeneratorSourceContract(_PolicyModel):
    schema_version: Literal["qqq_options_primary_window_derived_observation_bundle.v1"]
    derived_export_safe_required: Literal[True]
    raw_option_rows_prohibited: Literal[True]
    canonical_dq_report_required: Literal[True]
    exact_primary_session_inventory_required: Literal[True]
    incomplete_slot_rejected: Literal[True]
    deterministic_input_order_required: Literal[True]


class GeneratorSafety(_StrictModel):
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsDerivedCalibrationEvidenceGeneratorPolicy(_PolicyModel):
    schema_version: Literal[
        "qqq_options_primary_window_derived_calibration_evidence_generator_policy.v1"
    ]
    policy_id: Literal["qqq_options_primary_window_derived_calibration_evidence_generator_v1"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REVIEW_REQUIRED_ENGINEERING_BASELINE"]
    task_id: Literal[
        "TRADING-2511_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_GENERATOR_V1"
    ]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    upstream_calibration: GeneratorUpstreamCalibration
    source_contract: GeneratorSourceContract
    production_source_inventory: tuple[str, ...]
    metric_definitions: tuple[GeneratorMetricDefinition, ...]
    safety: GeneratorSafety

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start must remain 2021-02-22")
        if self.production_source_inventory:
            raise ValueError(
                "production source inventory must remain empty until separately reviewed"
            )
        slots = tuple(item.slot_id for item in self.metric_definitions)
        if slots != _EXPECTED_SLOT_IDS:
            raise ValueError("metric definitions must cover the exact sorted 18-slot G3 scope")
        metric_ids = tuple(item.metric_definition_id for item in self.metric_definitions)
        if len(metric_ids) != len(set(metric_ids)):
            raise ValueError("metric definition ids must be unique")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QQQOptionsDerivedCalibrationEvidenceGeneratorPolicyLoadResult:
    policy: QQQOptionsDerivedCalibrationEvidenceGeneratorPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


class DerivedSessionStatistic(_StrictModel):
    statistic_id: str
    value: str
    unit_id: str
    sample_count: int = Field(ge=1)
    is_policy_value: Literal[False]

    @field_validator("statistic_id", "unit_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value: str) -> str:
        _decimal(value, "value")
        return value


class DerivedSlotSessionObservation(_StrictModel):
    slot_id: str
    session_id: date
    statistics: tuple[DerivedSessionStatistic, ...]
    derived_export_safe: Literal[True]
    contains_raw_option_rows: Literal[False]

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @model_validator(mode="after")
    def _validate_statistics(self) -> Self:
        ids = tuple(item.statistic_id for item in self.statistics)
        if not ids or ids != tuple(sorted(ids)) or len(ids) != len(set(ids)):
            raise ValueError("observation statistics must be non-empty, unique, and sorted")
        return self


class QQQOptionsPrimaryWindowDerivedObservationBundle(_SealedModel):
    schema_version: Literal["qqq_options_primary_window_derived_observation_bundle.v1"]
    bundle_id: str
    created_at_utc: datetime
    repository_code_sha: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    session_ids: tuple[date, ...]
    provider_id: str
    dataset_id: str
    source_checksum: str
    dq_report_path: str
    dq_report_file_sha256: str
    dq_report_content_sha256: str
    observations: tuple[DerivedSlotSessionObservation, ...]
    derived_export_safe: Literal[True]
    contains_raw_option_rows: Literal[False]
    raw_options_data_exported: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]

    @field_validator("bundle_id", "provider_id", "dataset_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator("source_checksum", "dq_report_file_sha256", "dq_report_content_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("dq_report_path")
    @classmethod
    def _validate_dq_path(cls, value: str) -> str:
        return _require_relative_path(value, "dq_report_path")

    @model_validator(mode="after")
    def _validate_bundle(self) -> Self:
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY requested_start must remain 2021-02-22")
        if self.evaluated_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY evaluated_start must remain 2021-02-22")
        if not (
            self.requested_start
            <= self.evaluated_start
            <= self.evaluated_end
            <= self.requested_end
        ):
            raise ValueError("evaluated range must be contained in requested range")
        expected_sessions = _trading_sessions(self.evaluated_start, self.evaluated_end)
        if not expected_sessions or self.session_ids != expected_sessions:
            raise ValueError("session inventory differs from reviewed XNYS calendar")
        keys = tuple((item.slot_id, item.session_id) for item in self.observations)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("observations must be unique and sorted by slot/session")
        if any(item.session_id not in self.session_ids for item in self.observations):
            raise ValueError("observation session is outside the exact evaluated inventory")
        return self


class GeneratedArtifactIdentity(_StrictModel):
    path: str
    file_sha256: str
    content_sha256: str

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        return _require_relative_path(value, "path")

    @field_validator("file_sha256", "content_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class QQQOptionsDerivedCalibrationEvidenceReferenceIndex(_SealedModel):
    schema_version: Literal["qqq_options_derived_calibration_evidence_reference_index.v1"]
    package_id: str
    generated_at_utc: datetime
    implementation_repository_code_sha: str
    generator_policy_file_sha256: str
    generator_policy_canonical_sha256: str
    upstream_policy_file_sha256: str
    upstream_policy_canonical_sha256: str
    source_bundle: GeneratedArtifactIdentity
    evidence_references: tuple[CalibrationEvidenceReference, ...]
    safety: GeneratorSafety

    @field_validator("package_id")
    @classmethod
    def _validate_package_id(cls, value: str) -> str:
        return _identifier(value, "package_id")

    @field_validator("generated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "generated_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator(
        "generator_policy_file_sha256",
        "generator_policy_canonical_sha256",
        "upstream_policy_file_sha256",
        "upstream_policy_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_references(self) -> Self:
        slots = tuple(item.slot_id for item in self.evidence_references)
        if slots != tuple(sorted(slots)) or len(slots) != len(set(slots)):
            raise ValueError("evidence references must be unique and sorted")
        return self


class QQQOptionsDerivedCalibrationEvidencePackageManifest(_SealedModel):
    schema_version: Literal["qqq_options_derived_calibration_evidence_package_manifest.v1"]
    package_id: str
    generated_at_utc: datetime
    implementation_repository_code_sha: str
    source_bundle_content_sha256: str
    reference_index_content_sha256: str
    calibration_catalog_content_sha256: str
    calibration_receipt_content_sha256: str
    calibration_readiness_content_sha256: str
    calibration_handoff_content_sha256: str
    generated_slot_count: int = Field(ge=0, le=18)
    exact_file_inventory: tuple[str, ...]
    readiness_status: str
    owner_policy_value_count: Literal[0]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("package_id", "readiness_status", "engine_status")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("generated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "generated_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator(
        "source_bundle_content_sha256",
        "reference_index_content_sha256",
        "calibration_catalog_content_sha256",
        "calibration_receipt_content_sha256",
        "calibration_readiness_content_sha256",
        "calibration_handoff_content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("exact_file_inventory")
    @classmethod
    def _validate_inventory(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_require_relative_path(item, "exact_file_inventory") for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("exact file inventory must be unique and sorted")
        return checked


@dataclass(frozen=True)
class QQQOptionsDerivedCalibrationEvidencePackage:
    source_bundle: QQQOptionsPrimaryWindowDerivedObservationBundle
    evidence_records: tuple[QQQOptionsPrimaryWindowCalibrationEvidenceRecord, ...]
    reference_index: QQQOptionsDerivedCalibrationEvidenceReferenceIndex
    calibration_evaluation: QQQOptionsPrimaryWindowCalibrationEvaluation
    manifest: QQQOptionsDerivedCalibrationEvidencePackageManifest


def load_qqq_options_derived_calibration_evidence_generator_policy(
    path: Path = (
        DEFAULT_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_GENERATOR_POLICY_PATH
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDerivedCalibrationEvidenceGeneratorPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _bound_path(path, root=root, field="generator policy", must_exist=True)
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("generator policy root must be a mapping")
        policy = QQQOptionsDerivedCalibrationEvidenceGeneratorPolicy.model_validate(
            payload, strict=False
        )
        upstream = load_qqq_options_primary_window_calibration_policy(project_root=root)
        declared = policy.upstream_calibration
        if (
            declared.policy_path != upstream.policy_path.relative_to(root).as_posix()
            or declared.policy_file_sha256 != upstream.policy_file_sha256
            or declared.policy_canonical_sha256 != upstream.policy_canonical_sha256
        ):
            raise ValueError("2510 upstream calibration authority drifted")
    except QQQOptionsDerivedCalibrationEvidenceGeneratorError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_AUTHORITY_BINDING_MISMATCH", str(exc)
        ) from exc
    return QQQOptionsDerivedCalibrationEvidenceGeneratorPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def build_qqq_options_primary_window_derived_observation_bundle(
    *,
    bundle_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    requested_start: date,
    requested_end: date,
    evaluated_start: date,
    evaluated_end: date,
    provider_id: str,
    dataset_id: str,
    source_checksum: str,
    dq_report_path: str,
    dq_report_file_sha256: str,
    dq_report_content_sha256: str,
    observations: tuple[DerivedSlotSessionObservation, ...],
) -> QQQOptionsPrimaryWindowDerivedObservationBundle:
    sessions = _trading_sessions(evaluated_start, evaluated_end)
    return QQQOptionsPrimaryWindowDerivedObservationBundle.seal(
        schema_version="qqq_options_primary_window_derived_observation_bundle.v1",
        bundle_id=bundle_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        requested_start=requested_start,
        requested_end=requested_end,
        evaluated_start=evaluated_start,
        evaluated_end=evaluated_end,
        primary_research_role="PRIMARY",
        exchange_calendar="XNYS",
        session_ids=sessions,
        provider_id=provider_id,
        dataset_id=dataset_id,
        source_checksum=source_checksum,
        dq_report_path=dq_report_path,
        dq_report_file_sha256=dq_report_file_sha256,
        dq_report_content_sha256=dq_report_content_sha256,
        observations=tuple(sorted(observations, key=lambda item: (item.slot_id, item.session_id))),
        derived_export_safe=True,
        contains_raw_option_rows=False,
        raw_options_data_exported=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
    )


def _validate_policy_against_calibration_catalog(
    *,
    loaded: QQQOptionsDerivedCalibrationEvidenceGeneratorPolicyLoadResult,
    implementation_repository_code_sha: str,
    generated_at_utc: datetime,
    project_root: Path,
) -> None:
    baseline = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id="trading-2511-generator-policy-binding",
        issued_at_utc=generated_at_utc,
        implementation_repository_code_sha=implementation_repository_code_sha,
        project_root=project_root,
    )
    expected = tuple(
        (item.slot_id, item.canonical_group, item.evidence_class)
        for item in baseline.catalog.requirements
    )
    actual = tuple(
        (item.slot_id, item.canonical_group, item.evidence_class)
        for item in loaded.policy.metric_definitions
    )
    if actual != expected:
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_SLOT_AUTHORITY_MISMATCH",
            "generator metric scope differs from the canonical 2510 G3 catalog",
        )


def _validate_source_dq(
    *,
    source_bundle: QQQOptionsPrimaryWindowDerivedObservationBundle,
    evidence_root: Path,
    project_root: Path,
) -> None:
    try:
        path = _bound_path(
            Path(source_bundle.dq_report_path),
            root=evidence_root,
            field="DQ report path",
            must_exist=True,
        )
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != source_bundle.dq_report_file_sha256:
            raise ValueError("DQ report file SHA-256 mismatch")
        report = DQReportRecord.from_json_bytes(raw)
        if report.content_sha256 != source_bundle.dq_report_content_sha256:
            raise ValueError("DQ report content SHA-256 mismatch")
        upstream = load_qqq_options_primary_window_calibration_policy(project_root=project_root)
        dq = upstream.policy.dq_contract
        if (
            report.scope != dq.scope
            or report.report_version != dq.report_version
            or report.policy_id != dq.policy_id
            or report.policy_version != dq.policy_version
            or report.policy_sha256 != dq.policy_sha256
            or report.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256
            or report.repository_code_sha != source_bundle.repository_code_sha
            or report.dq_status != "PASS"
            or report.pit_status != "PASS"
        ):
            raise ValueError("DQ report authority or PASS status mismatch")
        if tuple(item.check_id for item in report.checks) != dq.required_check_ids or any(
            item.status != "PASS" for item in report.checks
        ):
            raise ValueError("DQ report is not exact 15-check PASS")
        if (
            report.requested_start,
            report.requested_end,
            report.evaluated_start,
            report.evaluated_end,
        ) != (
            source_bundle.requested_start,
            source_bundle.requested_end,
            source_bundle.evaluated_start,
            source_bundle.evaluated_end,
        ):
            raise ValueError("DQ report and source bundle ranges differ")
        if (source_bundle.dataset_id, source_bundle.source_checksum) not in tuple(
            zip(report.source_ids, report.source_checksums, strict=True)
        ):
            raise ValueError("DQ report source identity differs from source bundle")
        if report.generated_at_utc > source_bundle.created_at_utc:
            raise ValueError("DQ report is later than the derived source bundle")
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_DQ_REJECTED", str(exc)
        ) from exc


def _derive_evidence_records(
    *,
    source_bundle: QQQOptionsPrimaryWindowDerivedObservationBundle,
    generated_at_utc: datetime,
    loaded: QQQOptionsDerivedCalibrationEvidenceGeneratorPolicyLoadResult,
) -> tuple[QQQOptionsPrimaryWindowCalibrationEvidenceRecord, ...]:
    if generated_at_utc < source_bundle.created_at_utc:
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_AS_OF_MISMATCH", "generation time is earlier than source bundle creation"
        )
    observations_by_slot: dict[str, list[DerivedSlotSessionObservation]] = {}
    for observation in source_bundle.observations:
        observations_by_slot.setdefault(observation.slot_id, []).append(observation)
    unknown = sorted(set(observations_by_slot) - set(_EXPECTED_SLOT_IDS))
    if unknown:
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_SLOT_SCOPE_VIOLATION", f"unknown slot observations: {unknown}"
        )
    definitions = {item.slot_id: item for item in loaded.policy.metric_definitions}
    records: list[QQQOptionsPrimaryWindowCalibrationEvidenceRecord] = []
    for slot_id in sorted(observations_by_slot):
        definition = definitions[slot_id]
        observations = observations_by_slot[slot_id]
        sessions = tuple(item.session_id for item in observations)
        if sessions != source_bundle.session_ids:
            raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                "GENERATOR_INCOMPLETE_SLOT",
                f"{slot_id} does not cover the exact evaluated session inventory",
            )
        expected_stats = tuple(item.statistic_id for item in definition.statistics)
        for observation in observations:
            observed_stats = tuple(item.statistic_id for item in observation.statistics)
            if observed_stats != expected_stats:
                raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                    "GENERATOR_METRIC_DEFINITION_MISMATCH",
                    f"{slot_id}/{observation.session_id} statistic inventory drifted",
                )
            observed_units = tuple(item.unit_id for item in observation.statistics)
            expected_units = tuple(item.unit_id for item in definition.statistics)
            if observed_units != expected_units:
                raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
                    "GENERATOR_METRIC_DEFINITION_MISMATCH",
                    f"{slot_id}/{observation.session_id} statistic unit drifted",
                )
        aggregate_statistics: list[CalibrationAggregateStatistic] = []
        for statistic_definition in definition.statistics:
            source_statistics = tuple(
                next(
                    item
                    for item in observation.statistics
                    if item.statistic_id == statistic_definition.statistic_id
                )
                for observation in observations
            )
            values = tuple(_decimal(item.value, "value") for item in source_statistics)
            if statistic_definition.operation is DerivedStatisticOperation.SUM:
                aggregate_value = sum(values, Decimal(0))
            elif statistic_definition.operation is DerivedStatisticOperation.MIN:
                aggregate_value = min(values)
            else:
                aggregate_value = max(values)
            aggregate_statistics.append(
                CalibrationAggregateStatistic(
                    statistic_id=statistic_definition.statistic_id,
                    value=_format_decimal(aggregate_value),
                    unit_id=statistic_definition.unit_id,
                    sample_count=sum(item.sample_count for item in source_statistics),
                    is_policy_value=False,
                )
            )
        records.append(
            QQQOptionsPrimaryWindowCalibrationEvidenceRecord.seal(
                schema_version="qqq_options_primary_window_calibration_evidence.v1",
                record_id=f"{source_bundle.bundle_id}:{slot_id.lower()}:evidence",
                created_at_utc=generated_at_utc,
                repository_code_sha=source_bundle.repository_code_sha,
                slot_id=slot_id,
                evidence_class=definition.evidence_class,
                metric_definition_id=definition.metric_definition_id,
                metric_definition_sha256=definition.canonical_sha256,
                requested_start=source_bundle.requested_start,
                requested_end=source_bundle.requested_end,
                evaluated_start=source_bundle.evaluated_start,
                evaluated_end=source_bundle.evaluated_end,
                primary_research_role="PRIMARY",
                exchange_calendar="XNYS",
                session_ids=source_bundle.session_ids,
                as_of_session=source_bundle.session_ids[-1],
                provider_id=source_bundle.provider_id,
                dataset_id=source_bundle.dataset_id,
                source_checksum=source_bundle.source_checksum,
                statistics=tuple(aggregate_statistics),
                derived_export_safe=True,
                contains_raw_option_rows=False,
                raw_options_data_exported=False,
                external_action_performed=False,
                investment_interpretation_generated=False,
            )
        )
    return tuple(records)


def generate_qqq_options_primary_window_derived_calibration_evidence_package(
    *,
    package_id: str,
    generated_at_utc: datetime,
    source_bundle: QQQOptionsPrimaryWindowDerivedObservationBundle,
    evidence_root: Path,
    package_relative_path: str,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDerivedCalibrationEvidencePackage:
    generated_at = _utc(generated_at_utc, "generated_at_utc")
    root = evidence_root.resolve()
    package_relative = _require_relative_path(package_relative_path, "package_relative_path")
    package_dir = _bound_path(
        Path(package_relative), root=root, field="package directory", must_exist=False
    )
    if package_dir.exists():
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_PACKAGE_TARGET_EXISTS",
            "package target must not exist; overwrite and merge are prohibited",
        )
    loaded = load_qqq_options_derived_calibration_evidence_generator_policy(
        project_root=project_root
    )
    if source_bundle.repository_code_sha != _git_sha(
        source_bundle.repository_code_sha, "repository_code_sha"
    ):
        raise AssertionError("validated repository identity changed unexpectedly")
    _validate_policy_against_calibration_catalog(
        loaded=loaded,
        implementation_repository_code_sha=source_bundle.repository_code_sha,
        generated_at_utc=generated_at,
        project_root=project_root,
    )
    _validate_source_dq(
        source_bundle=source_bundle, evidence_root=root, project_root=project_root
    )
    evidence_records = _derive_evidence_records(
        source_bundle=source_bundle, generated_at_utc=generated_at, loaded=loaded
    )
    package_dir.mkdir(parents=True, exist_ok=False)
    evidence_dir = package_dir / "evidence"
    evidence_dir.mkdir()
    source_relative = f"{package_relative}/source_bundle.json"
    write_text_atomic(root / source_relative, source_bundle.canonical_bytes.decode("utf-8"))
    references: list[CalibrationEvidenceReference] = []
    for record in evidence_records:
        relative_path = f"{package_relative}/evidence/{record.slot_id}.json"
        write_text_atomic(root / relative_path, record.canonical_bytes.decode("utf-8"))
        references.append(
            CalibrationEvidenceReference(
                slot_id=record.slot_id,
                evidence_path=relative_path,
                evidence_file_sha256=record.canonical_sha256,
                evidence_content_sha256=record.content_sha256,
                dq_report_path=source_bundle.dq_report_path,
                dq_report_file_sha256=source_bundle.dq_report_file_sha256,
                dq_report_content_sha256=source_bundle.dq_report_content_sha256,
            )
        )
    evaluation = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id=package_id,
        issued_at_utc=generated_at,
        implementation_repository_code_sha=source_bundle.repository_code_sha,
        evidence_references=tuple(references),
        project_root=project_root,
        evidence_root=root,
    )
    upstream_files = {
        "calibration_catalog.json": evaluation.catalog,
        "calibration_receipt.json": evaluation.receipt,
        "calibration_readiness.json": evaluation.readiness,
        "calibration_handoff.json": evaluation.handoff,
    }
    for name, upstream_record in upstream_files.items():
        write_text_atomic(
            package_dir / name, upstream_record.canonical_bytes.decode("utf-8")
        )
    upstream = load_qqq_options_primary_window_calibration_policy(project_root=project_root)
    reference_index = QQQOptionsDerivedCalibrationEvidenceReferenceIndex.seal(
        schema_version="qqq_options_derived_calibration_evidence_reference_index.v1",
        package_id=package_id,
        generated_at_utc=generated_at,
        implementation_repository_code_sha=source_bundle.repository_code_sha,
        generator_policy_file_sha256=loaded.policy_file_sha256,
        generator_policy_canonical_sha256=loaded.policy_canonical_sha256,
        upstream_policy_file_sha256=upstream.policy_file_sha256,
        upstream_policy_canonical_sha256=upstream.policy_canonical_sha256,
        source_bundle=GeneratedArtifactIdentity(
            path=source_relative,
            file_sha256=source_bundle.canonical_sha256,
            content_sha256=source_bundle.content_sha256,
        ),
        evidence_references=tuple(references),
        safety=loaded.policy.safety,
    )
    index_relative = f"{package_relative}/reference_index.json"
    write_text_atomic(root / index_relative, reference_index.canonical_bytes.decode("utf-8"))
    inventory = tuple(
        sorted(
            (
                "calibration_catalog.json",
                "calibration_handoff.json",
                "calibration_readiness.json",
                "calibration_receipt.json",
                "package_manifest.json",
                "reference_index.json",
                "source_bundle.json",
                *(f"evidence/{item.slot_id}.json" for item in evidence_records),
            )
        )
    )
    manifest = QQQOptionsDerivedCalibrationEvidencePackageManifest.seal(
        schema_version="qqq_options_derived_calibration_evidence_package_manifest.v1",
        package_id=package_id,
        generated_at_utc=generated_at,
        implementation_repository_code_sha=source_bundle.repository_code_sha,
        source_bundle_content_sha256=source_bundle.content_sha256,
        reference_index_content_sha256=reference_index.content_sha256,
        calibration_catalog_content_sha256=evaluation.catalog.content_sha256,
        calibration_receipt_content_sha256=evaluation.receipt.content_sha256,
        calibration_readiness_content_sha256=evaluation.readiness.content_sha256,
        calibration_handoff_content_sha256=evaluation.handoff.content_sha256,
        generated_slot_count=len(evidence_records),
        exact_file_inventory=inventory,
        readiness_status=evaluation.readiness.readiness_status.value,
        owner_policy_value_count=0,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    write_text_atomic(
        package_dir / "package_manifest.json", manifest.canonical_bytes.decode("utf-8")
    )
    return QQQOptionsDerivedCalibrationEvidencePackage(
        source_bundle=source_bundle,
        evidence_records=evidence_records,
        reference_index=reference_index,
        calibration_evaluation=evaluation,
        manifest=manifest,
    )


def resolve_qqq_options_primary_window_derived_calibration_evidence_package(
    *,
    evidence_root: Path,
    package_relative_path: str,
    expected_implementation_repository_code_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDerivedCalibrationEvidencePackage:
    root = evidence_root.resolve()
    package_relative = _require_relative_path(package_relative_path, "package_relative_path")
    package_dir = _bound_path(
        Path(package_relative), root=root, field="package directory", must_exist=False
    )
    try:
        if not package_dir.is_dir():
            raise ValueError("package directory is missing")
        if any(path.is_symlink() for path in package_dir.rglob("*")):
            raise ValueError("package inventory cannot contain symlinks")
        manifest = QQQOptionsDerivedCalibrationEvidencePackageManifest.from_json_bytes(
            (package_dir / "package_manifest.json").read_bytes()
        )
        actual_inventory = tuple(
            sorted(
                path.relative_to(package_dir).as_posix()
                for path in package_dir.rglob("*")
                if path.is_file()
            )
        )
        if actual_inventory != manifest.exact_file_inventory:
            raise ValueError("package file inventory mismatch")
        if manifest.implementation_repository_code_sha != _git_sha(
            expected_implementation_repository_code_sha,
            "expected_implementation_repository_code_sha",
        ):
            raise ValueError("package repository identity mismatch")
        source_bundle = QQQOptionsPrimaryWindowDerivedObservationBundle.from_json_bytes(
            (package_dir / "source_bundle.json").read_bytes()
        )
        reference_index = QQQOptionsDerivedCalibrationEvidenceReferenceIndex.from_json_bytes(
            (package_dir / "reference_index.json").read_bytes()
        )
        if (
            source_bundle.content_sha256 != manifest.source_bundle_content_sha256
            or reference_index.content_sha256 != manifest.reference_index_content_sha256
            or reference_index.source_bundle.file_sha256 != source_bundle.canonical_sha256
            or reference_index.source_bundle.content_sha256 != source_bundle.content_sha256
        ):
            raise ValueError("source bundle or reference index identity mismatch")
        evidence_records = tuple(
            QQQOptionsPrimaryWindowCalibrationEvidenceRecord.from_json_bytes(
                (root / reference.evidence_path).read_bytes()
            )
            for reference in reference_index.evidence_references
        )
        expected_records = _derive_evidence_records(
            source_bundle=source_bundle,
            generated_at_utc=manifest.generated_at_utc,
            loaded=load_qqq_options_derived_calibration_evidence_generator_policy(
                project_root=project_root
            ),
        )
        if evidence_records != expected_records:
            raise ValueError("evidence records differ from deterministic source replay")
        evaluation = resolve_qqq_options_primary_window_calibration_evaluation(
            catalog_bytes=(package_dir / "calibration_catalog.json").read_bytes(),
            receipt_bytes=(package_dir / "calibration_receipt.json").read_bytes(),
            readiness_bytes=(package_dir / "calibration_readiness.json").read_bytes(),
            handoff_bytes=(package_dir / "calibration_handoff.json").read_bytes(),
            expected_implementation_repository_code_sha=(
                expected_implementation_repository_code_sha
            ),
            project_root=project_root,
            evidence_root=root,
        )
        if (
            evaluation.catalog.content_sha256 != manifest.calibration_catalog_content_sha256
            or evaluation.receipt.content_sha256
            != manifest.calibration_receipt_content_sha256
            or evaluation.readiness.content_sha256
            != manifest.calibration_readiness_content_sha256
            or evaluation.handoff.content_sha256
            != manifest.calibration_handoff_content_sha256
            or len(evidence_records) != manifest.generated_slot_count
            or evaluation.readiness.readiness_status.value != manifest.readiness_status
        ):
            raise ValueError("manifest and calibration evaluation cross-binding mismatch")
    except QQQOptionsDerivedCalibrationEvidenceGeneratorError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsDerivedCalibrationEvidenceGeneratorError(
            "GENERATOR_PACKAGE_REPLAY_REJECTED", str(exc)
        ) from exc
    return QQQOptionsDerivedCalibrationEvidencePackage(
        source_bundle=source_bundle,
        evidence_records=evidence_records,
        reference_index=reference_index,
        calibration_evaluation=evaluation,
        manifest=manifest,
    )


__all__ = [
    "DEFAULT_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_GENERATOR_POLICY_PATH",
    "DerivedSessionStatistic",
    "DerivedSlotSessionObservation",
    "DerivedStatisticOperation",
    "GeneratorMetricDefinition",
    "GeneratorSafety",
    "GeneratorSourceContract",
    "GeneratorStatisticDefinition",
    "GeneratorUpstreamCalibration",
    "GeneratedArtifactIdentity",
    "QQQOptionsDerivedCalibrationEvidenceGeneratorError",
    "QQQOptionsDerivedCalibrationEvidenceGeneratorPolicy",
    "QQQOptionsDerivedCalibrationEvidenceGeneratorPolicyLoadResult",
    "QQQOptionsDerivedCalibrationEvidencePackage",
    "QQQOptionsDerivedCalibrationEvidencePackageManifest",
    "QQQOptionsDerivedCalibrationEvidenceReferenceIndex",
    "QQQOptionsPrimaryWindowDerivedObservationBundle",
    "build_qqq_options_primary_window_derived_observation_bundle",
    "generate_qqq_options_primary_window_derived_calibration_evidence_package",
    "load_qqq_options_derived_calibration_evidence_generator_policy",
    "resolve_qqq_options_primary_window_derived_calibration_evidence_package",
]
