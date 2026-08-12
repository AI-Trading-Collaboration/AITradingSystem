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

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQReportRecord,
)
from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    OwnerDecisionAction,
    OwnerDecisionCanonicalGroup,
    OwnerDecisionEvidenceClass,
)
from ai_trading_system.qqq_options_research.owner_decision_manifest_v2 import (
    build_qqq_options_owner_decision_catalog_v2_migration_receipt,
    load_qqq_options_owner_decision_manifest_v2_policy,
)
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_PATH = Path(
    "config/research/qqq_options_primary_window_policy_calibration_v1.yaml"
)

_PRIMARY_RESEARCH_START = date(2021, 2, 22)
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_CANONICAL_DECIMAL_PATTERN = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")

_EXPECTED_G3_SLOT_IDS = (
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

_EXPECTED_DQ_CHECK_IDS = (
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "evidence_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "order_fill_chronology",
    "prior_day_model_freshness",
    "provider_raw_checksum",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
)


class QQQOptionsPrimaryWindowCalibrationContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class CalibrationReadinessStatus(StrEnum):
    EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED = "EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED"
    PARTIAL_EVIDENCE_POLICY_BLOCKED = "PARTIAL_EVIDENCE_POLICY_BLOCKED"
    READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE = "READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE"


class CalibrationSlotEvidenceStatus(StrEnum):
    MISSING = "MISSING"
    ADMITTED = "ADMITTED"


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
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
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


def _require_bound_regular_file(path: Path, *, root: Path, field: str) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the evidence root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the evidence root")
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    if not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} resolves outside the evidence root") from exc
    return resolved


def _trading_sessions(start: date, end: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while current <= end:
        if us_equity_market_session(current).is_trading_day:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


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
        allow_unsealed = bool(
            info.context and info.context.get("calibration_allow_unsealed") is True
        )
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
            raise QQQOptionsPrimaryWindowCalibrationContractError(
                "CALIBRATION_PAYLOAD_MISMATCH",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        try:
            provisional = cls.model_validate(
                {**payload, "content_sha256": _UNSEALED_SHA256},
                context={"calibration_allow_unsealed": True},
            )
            return cls.model_validate(
                {**payload, "content_sha256": provisional.compute_content_sha256()}
            )
        except (TypeError, ValueError) as exc:
            raise QQQOptionsPrimaryWindowCalibrationContractError(
                "CALIBRATION_PAYLOAD_MISMATCH", str(exc)
            ) from exc

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = _duplicate_key_rejecting_json(raw)
            if not isinstance(decoded, dict):
                raise ValueError("record JSON root must be an object")
            record = cls.model_validate_json(raw)
        except (TypeError, ValueError) as exc:
            raise QQQOptionsPrimaryWindowCalibrationContractError(
                "CALIBRATION_RECORD_INVALID", str(exc)
            ) from exc
        if record.canonical_bytes != raw:
            raise QQQOptionsPrimaryWindowCalibrationContractError(
                "CALIBRATION_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical UTF-8/LF JSON",
            )
        return record


class CalibrationSourceAuthority(_PolicyModel):
    v2_policy_path: str
    v2_policy_file_sha256: str
    v2_policy_canonical_sha256: str
    attestation_raw_sha256: str
    attestation_canonical_sha256: str

    @field_validator("v2_policy_path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        return _require_relative_path(value, "v2_policy_path")

    @field_validator(
        "v2_policy_file_sha256",
        "v2_policy_canonical_sha256",
        "attestation_raw_sha256",
        "attestation_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class CalibrationDQContract(_PolicyModel):
    scope: Literal["qqq_options_event_dq_pit_identity"]
    report_version: Literal["1.0.0"]
    policy_id: Literal["qqq_options_dq_pit_identity_v1"]
    policy_version: Literal["1.0.0"]
    policy_sha256: str
    required_check_ids: tuple[str, ...]

    @field_validator("policy_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "policy_sha256")

    @field_validator("required_check_ids")
    @classmethod
    def _validate_checks(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "required_check_ids") for item in value)
        if checked != _EXPECTED_DQ_CHECK_IDS:
            raise ValueError("required DQ check inventory drifted")
        return checked


class CalibrationEvidenceContract(_PolicyModel):
    schema_version: Literal["qqq_options_primary_window_calibration_evidence.v1"]
    derived_export_safe_required: Literal[True]
    raw_option_rows_prohibited: Literal[True]
    canonical_dq_pass_required: Literal[True]
    deterministic_input_order_required: Literal[True]


class CalibrationSafety(_StrictModel):
    maximum_status: Literal["READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsPrimaryWindowCalibrationPolicy(_PolicyModel):
    schema_version: Literal["qqq_options_primary_window_policy_calibration_policy.v1"]
    policy_id: Literal["qqq_options_primary_window_policy_calibration_v1"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["CALIBRATION_EVIDENCE_CONTRACT_ONLY"]
    task_id: Literal["TRADING-2510_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_EVIDENCE_V1"]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    source_authority: CalibrationSourceAuthority
    g3_slot_ids: tuple[str, ...]
    dq_contract: CalibrationDQContract
    evidence_contract: CalibrationEvidenceContract
    safety: CalibrationSafety

    @field_validator("g3_slot_ids")
    @classmethod
    def _validate_g3_slots(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "g3_slot_ids") for item in value)
        if checked != _EXPECTED_G3_SLOT_IDS:
            raise ValueError("exact G3 slot inventory drifted")
        return checked

    @model_validator(mode="after")
    def _validate_primary_window(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start must remain 2021-02-22")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QQQOptionsPrimaryWindowCalibrationPolicyLoadResult:
    policy: QQQOptionsPrimaryWindowCalibrationPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


class CalibrationAggregateStatistic(_StrictModel):
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
        if not _CANONICAL_DECIMAL_PATTERN.fullmatch(value):
            raise ValueError("value must be a canonical finite decimal string")
        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError("value must be a canonical finite decimal string") from exc
        if not parsed.is_finite():
            raise ValueError("value must be finite")
        return value


class QQQOptionsPrimaryWindowCalibrationEvidenceRecord(_SealedModel):
    schema_version: Literal["qqq_options_primary_window_calibration_evidence.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    slot_id: str
    evidence_class: OwnerDecisionEvidenceClass
    metric_definition_id: str
    metric_definition_sha256: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    session_ids: tuple[date, ...]
    as_of_session: date
    provider_id: str
    dataset_id: str
    source_checksum: str
    statistics: tuple[CalibrationAggregateStatistic, ...]
    derived_export_safe: Literal[True]
    contains_raw_option_rows: Literal[False]
    raw_options_data_exported: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]

    @field_validator(
        "record_id",
        "slot_id",
        "metric_definition_id",
        "provider_id",
        "dataset_id",
    )
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

    @field_validator("metric_definition_sha256", "source_checksum")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY evidence requested_start must remain 2021-02-22")
        if self.evaluated_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY evidence evaluated_start must remain 2021-02-22")
        if self.requested_end < self.requested_start:
            raise ValueError("requested range is reversed")
        if not (
            self.requested_start <= self.evaluated_start <= self.evaluated_end <= self.requested_end
        ):
            raise ValueError("evaluated range must be contained in requested range")
        expected_sessions = _trading_sessions(self.evaluated_start, self.evaluated_end)
        if not expected_sessions or self.session_ids != expected_sessions:
            raise ValueError("session inventory differs from reviewed XNYS calendar")
        if self.as_of_session != expected_sessions[-1]:
            raise ValueError("as_of_session must equal the final reviewed XNYS session")
        statistic_ids = tuple(item.statistic_id for item in self.statistics)
        if not statistic_ids or statistic_ids != tuple(sorted(statistic_ids)):
            raise ValueError("statistics must be non-empty and sorted")
        if len(statistic_ids) != len(set(statistic_ids)):
            raise ValueError("statistics must be unique")
        return self


class CalibrationEvidenceReference(_StrictModel):
    slot_id: str
    evidence_path: str
    evidence_file_sha256: str
    evidence_content_sha256: str
    dq_report_path: str
    dq_report_file_sha256: str
    dq_report_content_sha256: str

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @field_validator("evidence_path", "dq_report_path")
    @classmethod
    def _validate_paths(cls, value: str, info: ValidationInfo) -> str:
        return _require_relative_path(value, str(info.field_name))

    @field_validator(
        "evidence_file_sha256",
        "evidence_content_sha256",
        "dq_report_file_sha256",
        "dq_report_content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class CalibrationEvidenceRequirement(_StrictModel):
    slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    evidence_class: OwnerDecisionEvidenceClass
    requires: tuple[str, ...]
    blocks: tuple[str, ...]

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @field_validator("requires", "blocks")
    @classmethod
    def _validate_dependencies(
        cls, value: tuple[str, ...], info: ValidationInfo
    ) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError(f"{info.field_name} must be non-empty and unique")
        return tuple(_identifier(item, str(info.field_name)) for item in value)


class QQQOptionsPrimaryWindowCalibrationRequirementCatalog(_SealedModel):
    schema_version: Literal["qqq_options_primary_window_calibration_catalog.v1"]
    evaluation_id: str
    issued_at_utc: datetime
    implementation_repository_code_sha: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    v2_policy_file_sha256: str
    v2_policy_canonical_sha256: str
    attestation_raw_sha256: str
    attestation_canonical_sha256: str
    primary_research_start: date
    required_slot_count: Literal[18]
    requirements: tuple[CalibrationEvidenceRequirement, ...]
    safety: CalibrationSafety

    @field_validator("evaluation_id")
    @classmethod
    def _validate_evaluation_id(cls, value: str) -> str:
        return _identifier(value, "evaluation_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "v2_policy_file_sha256",
        "v2_policy_canonical_sha256",
        "attestation_raw_sha256",
        "attestation_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_catalog(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start drifted")
        slot_ids = tuple(item.slot_id for item in self.requirements)
        if slot_ids != _EXPECTED_G3_SLOT_IDS:
            raise ValueError("catalog does not contain the exact canonical G3 scope")
        return self


class AdmittedCalibrationEvidence(_StrictModel):
    slot_id: str
    evidence_class: OwnerDecisionEvidenceClass
    evidence_path: str
    evidence_file_sha256: str
    evidence_content_sha256: str
    evidence_canonical_sha256: str
    dq_report_path: str
    dq_report_file_sha256: str
    dq_report_content_sha256: str
    dq_report_canonical_sha256: str
    dq_record_id: str
    dq_lineage_id: str
    dq_passed_check_ids: tuple[str, ...]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    as_of_session: date
    provider_id: str
    dataset_id: str
    source_checksum: str
    metric_definition_id: str
    metric_definition_sha256: str

    @field_validator(
        "slot_id",
        "dq_record_id",
        "dq_lineage_id",
        "provider_id",
        "dataset_id",
        "metric_definition_id",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("evidence_path", "dq_report_path")
    @classmethod
    def _validate_paths(cls, value: str, info: ValidationInfo) -> str:
        return _require_relative_path(value, str(info.field_name))

    @field_validator(
        "evidence_file_sha256",
        "evidence_content_sha256",
        "evidence_canonical_sha256",
        "dq_report_file_sha256",
        "dq_report_content_sha256",
        "dq_report_canonical_sha256",
        "source_checksum",
        "metric_definition_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("dq_passed_check_ids")
    @classmethod
    def _validate_checks(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _EXPECTED_DQ_CHECK_IDS:
            raise ValueError("admitted DQ check inventory drifted")
        return value


class QQQOptionsPrimaryWindowCalibrationEvidenceBundleReceipt(_SealedModel):
    schema_version: Literal["qqq_options_primary_window_calibration_bundle_receipt.v1"]
    evaluation_id: str
    issued_at_utc: datetime
    implementation_repository_code_sha: str
    catalog_content_sha256: str
    catalog_canonical_sha256: str
    required_slot_count: Literal[18]
    admitted_slot_count: int = Field(ge=0, le=18)
    evidence_items: tuple[AdmittedCalibrationEvidence, ...]
    safety: CalibrationSafety

    @field_validator("evaluation_id")
    @classmethod
    def _validate_evaluation_id(cls, value: str) -> str:
        return _identifier(value, "evaluation_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator("catalog_content_sha256", "catalog_canonical_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        slot_ids = tuple(item.slot_id for item in self.evidence_items)
        if slot_ids != tuple(sorted(slot_ids)) or len(slot_ids) != len(set(slot_ids)):
            raise ValueError("admitted evidence must be unique and sorted by slot")
        if self.admitted_slot_count != len(slot_ids):
            raise ValueError("admitted slot count mismatch")
        if not set(slot_ids).issubset(_EXPECTED_G3_SLOT_IDS):
            raise ValueError("receipt contains a non-G3 slot")
        return self


class CalibrationSlotReadiness(_StrictModel):
    slot_id: str
    evidence_class: OwnerDecisionEvidenceClass
    status: CalibrationSlotEvidenceStatus
    evidence_content_sha256: str | None

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @field_validator("evidence_content_sha256")
    @classmethod
    def _validate_hash(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value, "evidence_content_sha256")

    @model_validator(mode="after")
    def _validate_status(self) -> Self:
        admitted = self.status is CalibrationSlotEvidenceStatus.ADMITTED
        if admitted != (self.evidence_content_sha256 is not None):
            raise ValueError("slot readiness and evidence identity disagree")
        return self


class QQQOptionsPrimaryWindowCalibrationReadinessReport(_SealedModel):
    schema_version: Literal["qqq_options_primary_window_calibration_readiness.v1"]
    evaluation_id: str
    issued_at_utc: datetime
    implementation_repository_code_sha: str
    receipt_content_sha256: str
    receipt_canonical_sha256: str
    readiness_status: CalibrationReadinessStatus
    required_slot_count: Literal[18]
    admitted_slot_count: int = Field(ge=0, le=18)
    missing_slot_count: int = Field(ge=0, le=18)
    slot_readiness: tuple[CalibrationSlotReadiness, ...]
    blocker_reason_codes: tuple[str, ...]
    safety: CalibrationSafety

    @field_validator("evaluation_id")
    @classmethod
    def _validate_evaluation_id(cls, value: str) -> str:
        return _identifier(value, "evaluation_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator("receipt_content_sha256", "receipt_canonical_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("blocker_reason_codes")
    @classmethod
    def _validate_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(value)) or len(value) != len(set(value)):
            raise ValueError("blocker reason codes must be non-empty, unique, and sorted")
        return tuple(_identifier(item, "blocker_reason_codes") for item in value)

    @model_validator(mode="after")
    def _validate_readiness(self) -> Self:
        slot_ids = tuple(item.slot_id for item in self.slot_readiness)
        if slot_ids != _EXPECTED_G3_SLOT_IDS:
            raise ValueError("readiness report does not cover the exact G3 scope")
        admitted = sum(
            item.status is CalibrationSlotEvidenceStatus.ADMITTED for item in self.slot_readiness
        )
        if self.admitted_slot_count != admitted:
            raise ValueError("admitted slot count mismatch")
        if self.missing_slot_count != self.required_slot_count - admitted:
            raise ValueError("missing slot count mismatch")
        expected_status = _readiness_status(admitted)
        if self.readiness_status is not expected_status:
            raise ValueError("readiness status does not match coverage")
        return self


class QQQOptionsPrimaryWindowOwnerReviewHandoff(_SealedModel):
    schema_version: Literal["qqq_options_primary_window_owner_review_handoff.v1"]
    evaluation_id: str
    issued_at_utc: datetime
    implementation_repository_code_sha: str
    readiness_content_sha256: str
    readiness_canonical_sha256: str
    readiness_status: CalibrationReadinessStatus
    required_slot_count: Literal[18]
    admitted_slot_count: int = Field(ge=0, le=18)
    missing_slot_count: int = Field(ge=0, le=18)
    review_disposition: Literal["NO_OWNER_POLICY_VALUES_EMITTED"]
    owner_review_required: Literal[True]
    evidence_review_is_not_policy_approval: Literal[True]
    safety: CalibrationSafety

    @field_validator("evaluation_id")
    @classmethod
    def _validate_evaluation_id(cls, value: str) -> str:
        return _identifier(value, "evaluation_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator("readiness_content_sha256", "readiness_canonical_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        if self.admitted_slot_count + self.missing_slot_count != self.required_slot_count:
            raise ValueError("handoff coverage counts do not sum to required scope")
        if self.readiness_status is not _readiness_status(self.admitted_slot_count):
            raise ValueError("handoff readiness status does not match coverage")
        return self


@dataclass(frozen=True)
class QQQOptionsPrimaryWindowCalibrationEvaluation:
    catalog: QQQOptionsPrimaryWindowCalibrationRequirementCatalog
    receipt: QQQOptionsPrimaryWindowCalibrationEvidenceBundleReceipt
    readiness: QQQOptionsPrimaryWindowCalibrationReadinessReport
    handoff: QQQOptionsPrimaryWindowOwnerReviewHandoff


def _readiness_status(admitted_count: int) -> CalibrationReadinessStatus:
    if admitted_count == 0:
        return CalibrationReadinessStatus.EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED
    if admitted_count < len(_EXPECTED_G3_SLOT_IDS):
        return CalibrationReadinessStatus.PARTIAL_EVIDENCE_POLICY_BLOCKED
    return CalibrationReadinessStatus.READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE


def _safe_policy() -> CalibrationSafety:
    return CalibrationSafety(
        maximum_status="READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        external_action_authorized=False,
        investment_interpretation_allowed=False,
        paper_allowed=False,
        live_allowed=False,
        broker_allowed=False,
        production_effect="none",
        broker_action="none",
    )


def load_qqq_options_primary_window_calibration_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsPrimaryWindowCalibrationPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(path, root=root, field="calibration policy")
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("calibration policy root must be a mapping")
        policy = QQQOptionsPrimaryWindowCalibrationPolicy.model_validate(payload, strict=False)
        v2 = load_qqq_options_owner_decision_manifest_v2_policy(project_root=root)
        source = policy.source_authority
        actual = {
            "v2_policy_file_sha256": v2.policy_file_sha256,
            "v2_policy_canonical_sha256": v2.policy_canonical_sha256,
            "attestation_raw_sha256": v2.policy.predecessor.attestation_raw_sha256,
            "attestation_canonical_sha256": (v2.policy.predecessor.attestation_canonical_sha256),
        }
        drifted = [
            field for field, observed in actual.items() if observed != getattr(source, field)
        ]
        if drifted:
            raise ValueError(f"calibration source authority drifted: {drifted}")
    except QQQOptionsPrimaryWindowCalibrationContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_AUTHORITY_BINDING_MISMATCH", str(exc)
        ) from exc
    return QQQOptionsPrimaryWindowCalibrationPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def _derive_catalog_requirements(
    *,
    repository_code_sha: str,
    issued_at_utc: datetime,
    project_root: Path,
) -> tuple[CalibrationEvidenceRequirement, ...]:
    migration = build_qqq_options_owner_decision_catalog_v2_migration_receipt(
        record_id="trading-2510-g3-scope-derivation",
        issued_at_utc=issued_at_utc,
        implementation_repository_code_sha=repository_code_sha,
        project_root=project_root,
    )
    requirements = tuple(
        CalibrationEvidenceRequirement(
            slot_id=item.successor_slot_id,
            canonical_group=item.canonical_group,
            evidence_class=item.evidence_class,
            requires=item.requires,
            blocks=item.blocks,
        )
        for item in migration.successor_slots
        if item.inherited_owner_action is OwnerDecisionAction.G3
    )
    slot_ids = tuple(item.slot_id for item in requirements)
    if slot_ids != _EXPECTED_G3_SLOT_IDS:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_G3_SCOPE_MISMATCH",
            "canonical attestation and v2 migration did not derive the exact 18-slot scope",
        )
    return requirements


def _validate_dq_report(
    *,
    reference: CalibrationEvidenceReference,
    evidence: QQQOptionsPrimaryWindowCalibrationEvidenceRecord,
    expected_repository_code_sha: str,
    policy: QQQOptionsPrimaryWindowCalibrationPolicy,
    evidence_root: Path,
) -> tuple[DQReportRecord, bytes]:
    try:
        path = _require_bound_regular_file(
            Path(reference.dq_report_path),
            root=evidence_root,
            field="DQ report path",
        )
        raw = path.read_bytes()
    except (OSError, ValueError) as exc:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_PATH_INVALID", str(exc)
        ) from exc
    if hashlib.sha256(raw).hexdigest() != reference.dq_report_file_sha256:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_FILE_HASH_MISMATCH", "DQ report file SHA-256 mismatch"
        )
    try:
        report = DQReportRecord.from_json_bytes(raw)
    except ValueError as exc:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_DQ_REJECTED", str(exc)
        ) from exc
    dq = policy.dq_contract
    if report.content_sha256 != reference.dq_report_content_sha256:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_FILE_HASH_MISMATCH", "DQ report content SHA-256 mismatch"
        )
    if (
        report.scope != dq.scope
        or report.report_version != dq.report_version
        or report.policy_id != dq.policy_id
        or report.policy_version != dq.policy_version
        or report.policy_sha256 != dq.policy_sha256
        or report.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256
        or report.repository_code_sha != expected_repository_code_sha
    ):
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_DQ_REJECTED", "DQ report authority identity mismatch"
        )
    report_range = (
        report.requested_start,
        report.requested_end,
        report.evaluated_start,
        report.evaluated_end,
    )
    evidence_range = (
        evidence.requested_start,
        evidence.requested_end,
        evidence.evaluated_start,
        evidence.evaluated_end,
    )
    if report_range != evidence_range:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_RANGE_MISMATCH", "DQ and evidence ranges differ"
        )
    if (
        report.generated_at_utc > evidence.created_at_utc
        or report.created_at_utc > evidence.created_at_utc
    ):
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_AS_OF_MISMATCH", "DQ report is later than evidence creation"
        )
    if report.dq_status != "PASS" or report.pit_status != "PASS":
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_DQ_REJECTED", "DQ and PIT must both derive as PASS"
        )
    check_ids = tuple(item.check_id for item in report.checks)
    if check_ids != dq.required_check_ids or any(item.status != "PASS" for item in report.checks):
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_DQ_REJECTED", "DQ report is not exact 15-check PASS"
        )
    source_pair = (evidence.dataset_id, evidence.source_checksum)
    if source_pair not in tuple(zip(report.source_ids, report.source_checksums, strict=True)):
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_SOURCE_MISMATCH", "DQ source identity or checksum mismatch"
        )
    return report, raw


def _admit_reference(
    *,
    reference: CalibrationEvidenceReference,
    requirement: CalibrationEvidenceRequirement,
    expected_repository_code_sha: str,
    policy: QQQOptionsPrimaryWindowCalibrationPolicy,
    evidence_root: Path,
) -> AdmittedCalibrationEvidence:
    try:
        path = _require_bound_regular_file(
            Path(reference.evidence_path),
            root=evidence_root,
            field="calibration evidence path",
        )
        raw = path.read_bytes()
    except (OSError, ValueError) as exc:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_PATH_INVALID", str(exc)
        ) from exc
    if hashlib.sha256(raw).hexdigest() != reference.evidence_file_sha256:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_FILE_HASH_MISMATCH", "evidence file SHA-256 mismatch"
        )
    evidence = QQQOptionsPrimaryWindowCalibrationEvidenceRecord.from_json_bytes(raw)
    if evidence.content_sha256 != reference.evidence_content_sha256:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_FILE_HASH_MISMATCH", "evidence content SHA-256 mismatch"
        )
    if evidence.slot_id != reference.slot_id or evidence.slot_id != requirement.slot_id:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_SLOT_SCOPE_VIOLATION", "evidence slot identity mismatch"
        )
    if evidence.evidence_class is not requirement.evidence_class:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_SLOT_SCOPE_VIOLATION", "evidence class differs from v2 authority"
        )
    if evidence.repository_code_sha != expected_repository_code_sha:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_AUTHORITY_BINDING_MISMATCH",
            "evidence repository identity differs from the expected tree",
        )
    report, dq_raw = _validate_dq_report(
        reference=reference,
        evidence=evidence,
        expected_repository_code_sha=expected_repository_code_sha,
        policy=policy,
        evidence_root=evidence_root,
    )
    return AdmittedCalibrationEvidence(
        slot_id=evidence.slot_id,
        evidence_class=evidence.evidence_class,
        evidence_path=reference.evidence_path,
        evidence_file_sha256=reference.evidence_file_sha256,
        evidence_content_sha256=evidence.content_sha256,
        evidence_canonical_sha256=hashlib.sha256(raw).hexdigest(),
        dq_report_path=reference.dq_report_path,
        dq_report_file_sha256=reference.dq_report_file_sha256,
        dq_report_content_sha256=report.content_sha256,
        dq_report_canonical_sha256=hashlib.sha256(dq_raw).hexdigest(),
        dq_record_id=report.record_id,
        dq_lineage_id=report.lineage_id,
        dq_passed_check_ids=tuple(item.check_id for item in report.checks),
        requested_start=evidence.requested_start,
        requested_end=evidence.requested_end,
        evaluated_start=evidence.evaluated_start,
        evaluated_end=evidence.evaluated_end,
        as_of_session=evidence.as_of_session,
        provider_id=evidence.provider_id,
        dataset_id=evidence.dataset_id,
        source_checksum=evidence.source_checksum,
        metric_definition_id=evidence.metric_definition_id,
        metric_definition_sha256=evidence.metric_definition_sha256,
    )


def build_qqq_options_primary_window_calibration_evaluation(
    *,
    evaluation_id: str,
    issued_at_utc: datetime,
    implementation_repository_code_sha: str,
    evidence_references: tuple[CalibrationEvidenceReference, ...] = (),
    project_root: Path = PROJECT_ROOT,
    evidence_root: Path | None = None,
) -> QQQOptionsPrimaryWindowCalibrationEvaluation:
    repository_sha = _git_sha(
        implementation_repository_code_sha, "implementation_repository_code_sha"
    )
    issued_at = _utc(issued_at_utc, "issued_at_utc")
    loaded = load_qqq_options_primary_window_calibration_policy(project_root=project_root)
    requirements = _derive_catalog_requirements(
        repository_code_sha=repository_sha,
        issued_at_utc=issued_at,
        project_root=project_root,
    )
    if tuple(item.slot_id for item in requirements) != loaded.policy.g3_slot_ids:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_G3_SCOPE_MISMATCH",
            "policy G3 inventory differs from canonical attestation migration",
        )
    safety = _safe_policy()
    source = loaded.policy.source_authority
    catalog = QQQOptionsPrimaryWindowCalibrationRequirementCatalog.seal(
        schema_version="qqq_options_primary_window_calibration_catalog.v1",
        evaluation_id=_identifier(evaluation_id, "evaluation_id"),
        issued_at_utc=issued_at,
        implementation_repository_code_sha=repository_sha,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        v2_policy_file_sha256=source.v2_policy_file_sha256,
        v2_policy_canonical_sha256=source.v2_policy_canonical_sha256,
        attestation_raw_sha256=source.attestation_raw_sha256,
        attestation_canonical_sha256=source.attestation_canonical_sha256,
        primary_research_start=_PRIMARY_RESEARCH_START,
        required_slot_count=18,
        requirements=requirements,
        safety=safety,
    )
    ordered_references = tuple(sorted(evidence_references, key=lambda item: item.slot_id))
    reference_ids = tuple(item.slot_id for item in ordered_references)
    if len(reference_ids) != len(set(reference_ids)):
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_DUPLICATE_SLOT", "evidence references contain a duplicate slot"
        )
    unknown = sorted(set(reference_ids) - set(_EXPECTED_G3_SLOT_IDS))
    if unknown:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_SLOT_SCOPE_VIOLATION",
            f"non-G3 evidence references are prohibited: {unknown}",
        )
    requirement_by_id = {item.slot_id: item for item in requirements}
    root = (evidence_root or project_root).resolve()
    admitted = tuple(
        _admit_reference(
            reference=reference,
            requirement=requirement_by_id[reference.slot_id],
            expected_repository_code_sha=repository_sha,
            policy=loaded.policy,
            evidence_root=root,
        )
        for reference in ordered_references
    )
    receipt = QQQOptionsPrimaryWindowCalibrationEvidenceBundleReceipt.seal(
        schema_version="qqq_options_primary_window_calibration_bundle_receipt.v1",
        evaluation_id=evaluation_id,
        issued_at_utc=issued_at,
        implementation_repository_code_sha=repository_sha,
        catalog_content_sha256=catalog.content_sha256,
        catalog_canonical_sha256=catalog.canonical_sha256,
        required_slot_count=18,
        admitted_slot_count=len(admitted),
        evidence_items=admitted,
        safety=safety,
    )
    admitted_by_id = {item.slot_id: item for item in admitted}
    slot_readiness = tuple(
        CalibrationSlotReadiness(
            slot_id=requirement.slot_id,
            evidence_class=requirement.evidence_class,
            status=(
                CalibrationSlotEvidenceStatus.ADMITTED
                if requirement.slot_id in admitted_by_id
                else CalibrationSlotEvidenceStatus.MISSING
            ),
            evidence_content_sha256=(
                admitted_by_id[requirement.slot_id].evidence_content_sha256
                if requirement.slot_id in admitted_by_id
                else None
            ),
        )
        for requirement in requirements
    )
    readiness_status = _readiness_status(len(admitted))
    blocker_reasons = (
        ("OWNER_REVIEWED_G2_POLICY_VALUES_NOT_PROVIDED",)
        if len(admitted) == len(requirements)
        else (
            "OWNER_REVIEWED_G2_POLICY_VALUES_NOT_PROVIDED",
            "PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_NOT_PROVIDED",
        )
    )
    readiness = QQQOptionsPrimaryWindowCalibrationReadinessReport.seal(
        schema_version="qqq_options_primary_window_calibration_readiness.v1",
        evaluation_id=evaluation_id,
        issued_at_utc=issued_at,
        implementation_repository_code_sha=repository_sha,
        receipt_content_sha256=receipt.content_sha256,
        receipt_canonical_sha256=receipt.canonical_sha256,
        readiness_status=readiness_status,
        required_slot_count=18,
        admitted_slot_count=len(admitted),
        missing_slot_count=len(requirements) - len(admitted),
        slot_readiness=slot_readiness,
        blocker_reason_codes=tuple(sorted(blocker_reasons)),
        safety=safety,
    )
    handoff = QQQOptionsPrimaryWindowOwnerReviewHandoff.seal(
        schema_version="qqq_options_primary_window_owner_review_handoff.v1",
        evaluation_id=evaluation_id,
        issued_at_utc=issued_at,
        implementation_repository_code_sha=repository_sha,
        readiness_content_sha256=readiness.content_sha256,
        readiness_canonical_sha256=readiness.canonical_sha256,
        readiness_status=readiness_status,
        required_slot_count=18,
        admitted_slot_count=len(admitted),
        missing_slot_count=len(requirements) - len(admitted),
        review_disposition="NO_OWNER_POLICY_VALUES_EMITTED",
        owner_review_required=True,
        evidence_review_is_not_policy_approval=True,
        safety=safety,
    )
    return QQQOptionsPrimaryWindowCalibrationEvaluation(
        catalog=catalog,
        receipt=receipt,
        readiness=readiness,
        handoff=handoff,
    )


def resolve_qqq_options_primary_window_calibration_evaluation(
    *,
    catalog_bytes: bytes,
    receipt_bytes: bytes,
    readiness_bytes: bytes,
    handoff_bytes: bytes,
    expected_implementation_repository_code_sha: str,
    project_root: Path = PROJECT_ROOT,
    evidence_root: Path | None = None,
) -> QQQOptionsPrimaryWindowCalibrationEvaluation:
    catalog = QQQOptionsPrimaryWindowCalibrationRequirementCatalog.from_json_bytes(catalog_bytes)
    receipt = QQQOptionsPrimaryWindowCalibrationEvidenceBundleReceipt.from_json_bytes(receipt_bytes)
    readiness = QQQOptionsPrimaryWindowCalibrationReadinessReport.from_json_bytes(readiness_bytes)
    handoff = QQQOptionsPrimaryWindowOwnerReviewHandoff.from_json_bytes(handoff_bytes)
    expected_sha = _git_sha(
        expected_implementation_repository_code_sha,
        "expected_implementation_repository_code_sha",
    )
    if any(
        item.implementation_repository_code_sha != expected_sha
        for item in (catalog, receipt, readiness, handoff)
    ):
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_AUTHORITY_BINDING_MISMATCH",
            "evaluation records differ from the expected repository tree",
        )
    references = tuple(
        CalibrationEvidenceReference(
            slot_id=item.slot_id,
            evidence_path=item.evidence_path,
            evidence_file_sha256=item.evidence_file_sha256,
            evidence_content_sha256=item.evidence_content_sha256,
            dq_report_path=item.dq_report_path,
            dq_report_file_sha256=item.dq_report_file_sha256,
            dq_report_content_sha256=item.dq_report_content_sha256,
        )
        for item in receipt.evidence_items
    )
    rebuilt = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id=catalog.evaluation_id,
        issued_at_utc=catalog.issued_at_utc,
        implementation_repository_code_sha=expected_sha,
        evidence_references=references,
        project_root=project_root,
        evidence_root=evidence_root,
    )
    observed = (
        catalog_bytes,
        receipt_bytes,
        readiness_bytes,
        handoff_bytes,
    )
    replayed = (
        rebuilt.catalog.canonical_bytes,
        rebuilt.receipt.canonical_bytes,
        rebuilt.readiness.canonical_bytes,
        rebuilt.handoff.canonical_bytes,
    )
    if observed != replayed:
        raise QQQOptionsPrimaryWindowCalibrationContractError(
            "CALIBRATION_REPLAY_MISMATCH",
            "calibration evaluation does not match deterministic replay",
        )
    return rebuilt


__all__ = [
    "DEFAULT_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_PATH",
    "AdmittedCalibrationEvidence",
    "CalibrationAggregateStatistic",
    "CalibrationEvidenceReference",
    "CalibrationEvidenceRequirement",
    "CalibrationReadinessStatus",
    "CalibrationSafety",
    "CalibrationSlotEvidenceStatus",
    "CalibrationSlotReadiness",
    "QQQOptionsPrimaryWindowCalibrationContractError",
    "QQQOptionsPrimaryWindowCalibrationEvaluation",
    "QQQOptionsPrimaryWindowCalibrationEvidenceBundleReceipt",
    "QQQOptionsPrimaryWindowCalibrationEvidenceRecord",
    "QQQOptionsPrimaryWindowCalibrationPolicy",
    "QQQOptionsPrimaryWindowCalibrationPolicyLoadResult",
    "QQQOptionsPrimaryWindowCalibrationReadinessReport",
    "QQQOptionsPrimaryWindowCalibrationRequirementCatalog",
    "QQQOptionsPrimaryWindowOwnerReviewHandoff",
    "build_qqq_options_primary_window_calibration_evaluation",
    "load_qqq_options_primary_window_calibration_policy",
    "resolve_qqq_options_primary_window_calibration_evaluation",
]
