from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.owner_stage_gate_signoff import (
    OwnerStageGateAxis,
    OwnerStageGateDecision,
    QCQQQOptionsOwnerStageGateOwnerAttestationRecord,
    QCQQQOptionsOwnerStageGateSignoffRecord,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH = Path(
    "config/research/qc_qqq_options_license_export_due_diligence_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_REPORT_PATH = Path(
    "inputs/external_validation/qc_qqq_options_license_export_due_diligence_report_20260807.json"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")


class LicenseEvidenceRole(StrEnum):
    DATASET_LICENSING = "DATASET_LICENSING"
    TIER_CAPABILITY = "TIER_CAPABILITY"
    RESOURCE_LIMITS = "RESOURCE_LIMITS"
    OPTION_DATASET_COVERAGE = "OPTION_DATASET_COVERAGE"
    DERIVED_RESULT_EXPORT = "DERIVED_RESULT_EXPORT"
    SITE_TERMS = "SITE_TERMS"


class LicenseClaimClassification(StrEnum):
    DOCUMENTED_FACT = "DOCUMENTED_FACT"
    CONSERVATIVE_INFERENCE = "CONSERVATIVE_INFERENCE"
    EXPLICIT_UNKNOWN = "EXPLICIT_UNKNOWN"


class LicenseAssessmentAxis(StrEnum):
    FREE_CLOUD_DATA_CLASS_ACCESS = "FREE_CLOUD_DATA_CLASS_ACCESS"
    QQQ_OPTIONS_ACCOUNT_ENTITLEMENT = "QQQ_OPTIONS_ACCOUNT_ENTITLEMENT"
    PRIMARY_WINDOW_HISTORICAL_RETENTION = "PRIMARY_WINDOW_HISTORICAL_RETENTION"
    RAW_OPTIONS_LOCAL_DOWNLOAD = "RAW_OPTIONS_LOCAL_DOWNLOAD"
    RAW_OPTIONS_REDISTRIBUTION = "RAW_OPTIONS_REDISTRIBUTION"
    DERIVED_BACKTEST_RESULT_EXPORT = "DERIVED_BACKTEST_RESULT_EXPORT"
    API_CLI_ACCESS = "API_CLI_ACCESS"


class LicenseAssessmentStatus(StrEnum):
    PUBLIC_DOCS_CONDITIONAL_SUPPORT = "PUBLIC_DOCS_CONDITIONAL_SUPPORT"
    UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED = "UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED"
    NO_GO_SEPARATE_DOWNLOAD_LICENSE_REQUIRED = "NO_GO_SEPARATE_DOWNLOAD_LICENSE_REQUIRED"
    NO_GO_PROHIBITED = "NO_GO_PROHIBITED"
    CONDITIONAL_DOCUMENTED_UI_EXPORT_ONLY = "CONDITIONAL_DOCUMENTED_UI_EXPORT_ONLY"
    NO_GO_CURRENT_FREE_TIER = "NO_GO_CURRENT_FREE_TIER"


EXPECTED_SOURCE_IDS: tuple[str, ...] = (
    "QC_DATASET_LICENSING_DOC",
    "QC_TIER_FEATURES_DOC",
    "QC_RESOURCES_DOC",
    "QC_US_EQUITY_OPTIONS_DOC",
    "QC_BACKTEST_RESULTS_DOC",
    "QC_SITE_TERMS",
)
EXPECTED_SOURCE_URLS: tuple[str, ...] = (
    "https://www.quantconnect.com/docs/v2/cloud-platform/datasets/licensing",
    "https://www.quantconnect.com/docs/v2/cloud-platform/organizations/tier-features",
    "https://www.quantconnect.com/docs/v2/cloud-platform/organizations/resources",
    "https://www.quantconnect.com/docs/v2/writing-algorithms/datasets/algoseek/us-equity-options",
    "https://www.quantconnect.com/docs/v2/cloud-platform/backtesting/results",
    "https://www.quantconnect.com/terms/",
)
EXPECTED_CLAIM_IDS: tuple[str, ...] = (
    "FREE_TIER_MINUTE_DAILY_CLOUD_DATA",
    "US_EQUITY_OPTIONS_MINUTE_PROVIDER_COVERAGE",
    "CLOUD_AND_DOWNLOAD_LICENSES_ARE_DISTINCT",
    "DOWNLOAD_IS_INTERNAL_LEAN_ONLY_NO_REDISTRIBUTION",
    "DERIVED_BACKTEST_UI_EXPORTS_ARE_DOCUMENTED",
    "API_CLI_IS_NOT_A_FREE_TIER_CAPABILITY",
    "ACCOUNT_QQQ_OPTIONS_ENTITLEMENT_NOT_PROVEN",
    "PRIMARY_WINDOW_RETENTION_NOT_PROVEN",
    "AUTOMATED_PAGE_COPY_NOT_ADMITTED",
)
EXPECTED_AXIS_STATUSES: tuple[tuple[LicenseAssessmentAxis, LicenseAssessmentStatus], ...] = (
    (
        LicenseAssessmentAxis.FREE_CLOUD_DATA_CLASS_ACCESS,
        LicenseAssessmentStatus.PUBLIC_DOCS_CONDITIONAL_SUPPORT,
    ),
    (
        LicenseAssessmentAxis.QQQ_OPTIONS_ACCOUNT_ENTITLEMENT,
        LicenseAssessmentStatus.UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED,
    ),
    (
        LicenseAssessmentAxis.PRIMARY_WINDOW_HISTORICAL_RETENTION,
        LicenseAssessmentStatus.UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED,
    ),
    (
        LicenseAssessmentAxis.RAW_OPTIONS_LOCAL_DOWNLOAD,
        LicenseAssessmentStatus.NO_GO_SEPARATE_DOWNLOAD_LICENSE_REQUIRED,
    ),
    (
        LicenseAssessmentAxis.RAW_OPTIONS_REDISTRIBUTION,
        LicenseAssessmentStatus.NO_GO_PROHIBITED,
    ),
    (
        LicenseAssessmentAxis.DERIVED_BACKTEST_RESULT_EXPORT,
        LicenseAssessmentStatus.CONDITIONAL_DOCUMENTED_UI_EXPORT_ONLY,
    ),
    (
        LicenseAssessmentAxis.API_CLI_ACCESS,
        LicenseAssessmentStatus.NO_GO_CURRENT_FREE_TIER,
    ),
)


class QCQQQOptionsLicenseExportDueDiligenceContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
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
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
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


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_sha256(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        semantic = cls.model_validate({**payload, "content_sha256": "0" * 64}).semantic_payload()
        return cls.model_validate({**payload, "content_sha256": _canonical_sha256(semantic)})

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("record is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("record JSON root must be an object")
        record = cls.model_validate(decoded)
        if record.canonical_bytes != raw:
            raise ValueError("record bytes are not canonical")
        if record.content_sha256 != _canonical_sha256(record.semantic_payload()):
            raise ValueError("record semantic content SHA-256 mismatch")
        return record


class LicenseAuthorityBinding(_StrictModel):
    authority_id: str
    relative_path: str
    sha256: str
    schema_version: str

    @field_validator("authority_id", "schema_version")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("relative_path")
    @classmethod
    def _validate_relative_path(cls, value: str) -> str:
        checked = _required_text(value, "relative_path")
        path = Path(checked)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != checked:
            raise ValueError("relative_path must be a normalized project-relative path")
        return checked

    @field_validator("sha256")
    @classmethod
    def _validate_sha(cls, value: str) -> str:
        return _sha256(value, "sha256")


class LicenseEvidenceSourceRecord(_StrictModel):
    source_id: str
    url: str
    role: LicenseEvidenceRole
    retrieved_on: date
    capture_mode: Literal["PUBLIC_REFERENCE_METADATA_ONLY_NO_PAGE_COPY"]
    source_content_checksum_status: Literal["NOT_CAPTURED_AUTOMATION_PROHIBITED"]
    source_content_sha256: None
    manual_review_status: Literal["PENDING_MANUAL_OWNER_REVIEW"]
    manual_reviewer: Literal["project_owner"]
    limitation: str

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        return _identifier(value, "source_id")

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        checked = _required_text(value, "url")
        parsed = urlsplit(checked)
        if parsed.scheme != "https" or parsed.netloc != "www.quantconnect.com":
            raise ValueError("url must use the exact official QuantConnect HTTPS host")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("url cannot contain query, fragment, or credentials")
        return checked

    @field_validator("retrieved_on")
    @classmethod
    def _validate_retrieved_on(cls, value: date) -> date:
        if value != date(2026, 8, 7):
            raise ValueError("retrieved_on must match the reviewed reference date")
        return value

    @field_validator("limitation")
    @classmethod
    def _validate_limitation(cls, value: str) -> str:
        return _required_text(value, "limitation")


class LicenseClaimRecord(_StrictModel):
    claim_id: str
    classification: LicenseClaimClassification
    source_ids: tuple[str, ...]
    summary: str
    allowed_conclusion: str
    owner: str
    exit_condition: str

    @field_validator("claim_id")
    @classmethod
    def _validate_claim_id(cls, value: str) -> str:
        return _identifier(value, "claim_id")

    @field_validator("source_ids")
    @classmethod
    def _validate_source_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("source_ids must be non-empty and unique")
        return tuple(_identifier(item, "source_id") for item in value)

    @field_validator("summary", "allowed_conclusion", "owner", "exit_condition")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))


class LicenseAxisAssessment(_StrictModel):
    axis_id: LicenseAssessmentAxis
    status: LicenseAssessmentStatus
    supporting_claim_ids: tuple[str, ...]
    blocker: str

    @field_validator("supporting_claim_ids")
    @classmethod
    def _validate_claim_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("supporting_claim_ids must be non-empty and unique")
        return tuple(_identifier(item, "supporting_claim_id") for item in value)

    @field_validator("blocker")
    @classmethod
    def _validate_blocker(cls, value: str) -> str:
        return _required_text(value, "blocker")


class QCQQQOptionsLicenseExportSafety(_StrictModel):
    quantconnect_login_performed: Literal[False]
    cloud_backtest_performed: Literal[False]
    project_mutation_performed: Literal[False]
    api_cli_http_object_store_used: Literal[False]
    raw_options_data_downloaded: Literal[False]
    range_expansion_allowed: Literal[False]
    paid_tier_upgrade_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action: Literal["none"]


class QCQQQOptionsLicenseExportDueDiligencePolicy(_StrictModel):
    schema_version: Literal["qc_qqq_options_license_export_due_diligence_policy.v1"]
    policy_id: str
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED"]
    effective_date: date
    predecessor_task_id: Literal["TRADING-2493_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_SIGNOFF_V1"]
    authority_bindings: tuple[LicenseAuthorityBinding, ...]
    sources: tuple[LicenseEvidenceSourceRecord, ...]
    claims: tuple[LicenseClaimRecord, ...]
    assessments: tuple[LicenseAxisAssessment, ...]
    aggregate_decision: Literal["LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED"]
    primary_research_window_start: date
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    safety: QCQQQOptionsLicenseExportSafety

    @field_validator("policy_id")
    @classmethod
    def _validate_policy_id(cls, value: str) -> str:
        return _identifier(value, "policy_id")

    @field_validator("primary_research_window_start")
    @classmethod
    def _validate_primary_window(cls, value: date) -> date:
        if value != date(2021, 2, 22):
            raise ValueError("primary_research_window_start must remain 2021-02-22")
        return value

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        authority_ids = tuple(item.authority_id for item in self.authority_bindings)
        if authority_ids != ("TRADING_2493_OWNER_ATTESTATION", "TRADING_2493_SIGNOFF"):
            raise ValueError("authority binding inventory must be exact and ordered")
        source_ids = tuple(item.source_id for item in self.sources)
        source_urls = tuple(item.url for item in self.sources)
        if source_ids != EXPECTED_SOURCE_IDS or source_urls != EXPECTED_SOURCE_URLS:
            raise ValueError("official source inventory or URL drifted")
        claim_ids = tuple(item.claim_id for item in self.claims)
        if claim_ids != EXPECTED_CLAIM_IDS:
            raise ValueError("claim inventory must be exact and ordered")
        known_sources = set(source_ids)
        for claim in self.claims:
            if not set(claim.source_ids) <= known_sources:
                raise ValueError("claim references unknown source ids")
        assessment_pairs = tuple((item.axis_id, item.status) for item in self.assessments)
        if assessment_pairs != EXPECTED_AXIS_STATUSES:
            raise ValueError("assessment inventory or status drifted")
        known_claims = set(claim_ids)
        for assessment in self.assessments:
            if not set(assessment.supporting_claim_ids) <= known_claims:
                raise ValueError("assessment references unknown claim ids")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QCQQQOptionsLicenseExportDueDiligencePolicyLoadResult:
    policy: QCQQQOptionsLicenseExportDueDiligencePolicy
    policy_path: Path
    policy_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str


class QCQQQOptionsLicenseExportDueDiligenceReport(_SealedModel):
    schema_version: Literal["qc_qqq_options_license_export_due_diligence_report.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str
    predecessor_owner_attestation_file_sha256: str
    predecessor_owner_attestation_content_sha256: str
    predecessor_signoff_file_sha256: str
    predecessor_signoff_content_sha256: str
    predecessor_signoff_status: Literal["SIGNED_NO_GO"]
    predecessor_aggregate_decision: Literal["NO_GO_KEEP_BLOCKED"]
    predecessor_license_export_axis: Literal["NO_GO"]
    sources: tuple[LicenseEvidenceSourceRecord, ...]
    claims: tuple[LicenseClaimRecord, ...]
    assessments: tuple[LicenseAxisAssessment, ...]
    aggregate_decision: Literal["LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED"]
    primary_research_window_start: date
    research_run_performed: Literal[False]
    requested_range: None
    evaluated_range: None
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    owner_review_status: Literal["PENDING_MANUAL_OWNER_REVIEW"]
    legal_opinion_provided: Literal[False]
    safety: QCQQQOptionsLicenseExportSafety

    @field_validator("record_id", "policy_id")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        checked = _utc(value, "created_at_utc")
        if checked.date() < date(2026, 8, 6):
            raise ValueError("report predates the due-diligence task")
        return checked

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator("primary_research_window_start")
    @classmethod
    def _validate_primary_window(cls, value: date) -> date:
        if value != date(2021, 2, 22):
            raise ValueError("primary_research_window_start must remain 2021-02-22")
        return value

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "authority_set_sha256",
        "predecessor_owner_attestation_file_sha256",
        "predecessor_owner_attestation_content_sha256",
        "predecessor_signoff_file_sha256",
        "predecessor_signoff_content_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_report(self) -> Self:
        if tuple(item.source_id for item in self.sources) != EXPECTED_SOURCE_IDS:
            raise ValueError("report source inventory drifted")
        if tuple(item.claim_id for item in self.claims) != EXPECTED_CLAIM_IDS:
            raise ValueError("report claim inventory drifted")
        pairs = tuple((item.axis_id, item.status) for item in self.assessments)
        if pairs != EXPECTED_AXIS_STATUSES:
            raise ValueError("report assessment inventory or status drifted")
        if any(item.source_content_sha256 is not None for item in self.sources):
            raise ValueError("report cannot pretend that source page bytes were captured")
        return self


def _require_bound_regular_file(path: Path, *, project_root: Path, field: str) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    if candidate.is_symlink():
        raise ValueError(f"{field} cannot use a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    return resolved


def load_qc_qqq_options_license_export_due_diligence_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsLicenseExportDueDiligencePolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="license/export policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("license/export policy root must be a mapping")
        policy = QCQQQOptionsLicenseExportDueDiligencePolicy.model_validate(payload)
        authority_payload: list[dict[str, str]] = []
        for binding in policy.authority_bindings:
            bound_path = _require_bound_regular_file(
                Path(binding.relative_path),
                project_root=root,
                field=f"authority {binding.authority_id}",
            )
            actual = sha256_path(bound_path)
            if actual != binding.sha256:
                raise ValueError(
                    f"authority {binding.authority_id} SHA-256 mismatch: expected "
                    f"{binding.sha256}, observed {actual}"
                )
            authority_payload.append(binding.model_dump(mode="json"))
    except QCQQQOptionsLicenseExportDueDiligenceContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsLicenseExportDueDiligenceContractError(
            "QC_QQQ_OPTIONS_LICENSE_EXPORT_POLICY_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsLicenseExportDueDiligencePolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        authority_set_sha256=_canonical_sha256(authority_payload),
    )


def build_qc_qqq_options_license_export_due_diligence_report(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsLicenseExportDueDiligenceReport:
    root = project_root.resolve()
    loaded = load_qc_qqq_options_license_export_due_diligence_policy(policy_path, project_root=root)
    bindings = {item.authority_id: item for item in loaded.policy.authority_bindings}
    try:
        attestation_path = _require_bound_regular_file(
            Path(bindings["TRADING_2493_OWNER_ATTESTATION"].relative_path),
            project_root=root,
            field="TRADING-2493 Owner attestation",
        )
        signoff_path = _require_bound_regular_file(
            Path(bindings["TRADING_2493_SIGNOFF"].relative_path),
            project_root=root,
            field="TRADING-2493 signoff",
        )
        attestation_raw = attestation_path.read_bytes()
        signoff_raw = signoff_path.read_bytes()
        attestation = QCQQQOptionsOwnerStageGateOwnerAttestationRecord.from_json_bytes(
            attestation_raw
        )
        signoff = QCQQQOptionsOwnerStageGateSignoffRecord.from_json_bytes(signoff_raw)
        license_decisions = {item.axis_id: item.decision for item in signoff.axis_decisions}
        if (
            signoff.signoff_status != "SIGNED_NO_GO"
            or signoff.aggregate_decision != "NO_GO_KEEP_BLOCKED"
            or license_decisions.get(OwnerStageGateAxis.LICENSE_EXPORT)
            != OwnerStageGateDecision.NO_GO
            or not attestation.confirmed_no_external_action
            or signoff.safety.further_cloud_action_authorized
            or signoff.safety.paid_tier_upgrade_authorized
        ):
            raise ValueError("TRADING-2493 signed NO-GO safety authority drifted")
    except QCQQQOptionsLicenseExportDueDiligenceContractError:
        raise
    except (KeyError, OSError, ValueError) as exc:
        raise QCQQQOptionsLicenseExportDueDiligenceContractError(
            "QC_QQQ_OPTIONS_LICENSE_EXPORT_PREDECESSOR_INVALID", str(exc)
        ) from exc

    policy = loaded.policy
    return QCQQQOptionsLicenseExportDueDiligenceReport.seal(
        schema_version="qc_qqq_options_license_export_due_diligence_report.v1",
        record_id=record_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_file_sha256=loaded.policy_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        authority_set_sha256=loaded.authority_set_sha256,
        predecessor_owner_attestation_file_sha256=hashlib.sha256(attestation_raw).hexdigest(),
        predecessor_owner_attestation_content_sha256=attestation.content_sha256,
        predecessor_signoff_file_sha256=hashlib.sha256(signoff_raw).hexdigest(),
        predecessor_signoff_content_sha256=signoff.content_sha256,
        predecessor_signoff_status="SIGNED_NO_GO",
        predecessor_aggregate_decision="NO_GO_KEEP_BLOCKED",
        predecessor_license_export_axis="NO_GO",
        sources=policy.sources,
        claims=policy.claims,
        assessments=policy.assessments,
        aggregate_decision=policy.aggregate_decision,
        primary_research_window_start=policy.primary_research_window_start,
        research_run_performed=False,
        requested_range=None,
        evaluated_range=None,
        option_event_dq_status=policy.option_event_dq_status,
        option_event_pit_status=policy.option_event_pit_status,
        owner_review_status="PENDING_MANUAL_OWNER_REVIEW",
        legal_opinion_provided=False,
        safety=policy.safety,
    )
