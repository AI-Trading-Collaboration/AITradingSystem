from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.license_export_due_diligence import (
    LicenseAssessmentAxis,
    QCQQQOptionsLicenseExportDueDiligenceReport,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH = Path(
    "config/research/qc_qqq_options_license_export_owner_review_proposal_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_PROPOSAL_PATH = Path(
    "inputs/external_validation/qc_qqq_options_license_export_owner_review_proposal_20260807.json"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_UNSEALED_SHA256 = "0" * 64


class OwnerManualEvidenceRole(StrEnum):
    ACCOUNT_TIER = "ACCOUNT_TIER"
    PLAN_RESOURCE_LIMITS = "PLAN_RESOURCE_LIMITS"
    DATASET_PRICING = "DATASET_PRICING"


class OwnerReviewRecommendation(StrEnum):
    CONDITIONAL_GO_PUBLIC_FREE_AND_ACCOUNT_FREE = "CONDITIONAL_GO_PUBLIC_FREE_AND_ACCOUNT_FREE"
    CONDITIONAL_GO_TESTED_SESSION_ONLY = "CONDITIONAL_GO_TESTED_SESSION_ONLY"
    NO_GO_NOT_TESTED_ACCOUNT_SPECIFIC = "NO_GO_NOT_TESTED_ACCOUNT_SPECIFIC"
    NO_GO_SEPARATE_PAID_DOWNLOAD_NOT_AUTHORIZED = "NO_GO_SEPARATE_PAID_DOWNLOAD_NOT_AUTHORIZED"
    NO_GO_PROHIBITED = "NO_GO_PROHIBITED"
    NO_GO_PENDING_TRADING_2489 = "NO_GO_PENDING_TRADING_2489"
    NO_GO_FREE_TIER = "NO_GO_FREE_TIER"


EXPECTED_EVIDENCE: tuple[tuple[str, int, str, OwnerManualEvidenceRole], ...] = (
    (
        "OWNER_ACCOUNT_ACTIVE_ORG_FREE_SCREENSHOT",
        526327,
        "b06125aadd2353e3e12d54190e0d5ae84b10d10ac34124dca9dbb8aaef43d724",
        OwnerManualEvidenceRole.ACCOUNT_TIER,
    ),
    (
        "OWNER_ORG_PLAN_RESOURCE_PRINT_PDF",
        579556,
        "13ded5b86d015bafb7709b47ccf239b47bfe42eaa93e2d4f0d7b86ff19fdaa1d",
        OwnerManualEvidenceRole.PLAN_RESOURCE_LIMITS,
    ),
    (
        "OWNER_US_EQUITY_OPTIONS_PRICING_1",
        340575,
        "4dc85d89db6f5290d23a651242d4ebd5fcce3b6423d186778fd6f9cc3295cdbf",
        OwnerManualEvidenceRole.DATASET_PRICING,
    ),
    (
        "OWNER_US_EQUITY_OPTIONS_PRICING_2",
        309760,
        "97ce21bb4744e40b0b54749ec26f2fd8c55902a9c0532e1e7cd42e0da3852de9",
        OwnerManualEvidenceRole.DATASET_PRICING,
    ),
    (
        "OWNER_US_EQUITY_OPTIONS_PRICING_3",
        256495,
        "26aceca998ebc7548f325483149abb4d1700c97f068594c0becfff413a828acb",
        OwnerManualEvidenceRole.DATASET_PRICING,
    ),
    (
        "OWNER_US_EQUITY_OPTIONS_PRICING_4",
        250607,
        "bfc321de857522e99073c2917199a21cf76251324914f3ef01ee607f7ac8b11a",
        OwnerManualEvidenceRole.DATASET_PRICING,
    ),
)

EXPECTED_LISTING_FACTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("CLOUD_ACCESS", "Free", ("OWNER_US_EQUITY_OPTIONS_PRICING_1",)),
    ("MINUTE_DOWNLOAD", "15 QCC/file", ("OWNER_US_EQUITY_OPTIONS_PRICING_1",)),
    ("HOUR_DOWNLOAD", "900 QCC/file", ("OWNER_US_EQUITY_OPTIONS_PRICING_2",)),
    ("DAILY_DOWNLOAD", "300 QCC/file", ("OWNER_US_EQUITY_OPTIONS_PRICING_2",)),
    ("BULK_DAILY_UPDATES", "$720/yr", ("OWNER_US_EQUITY_OPTIONS_PRICING_3",)),
    ("BULK_MINUTE_UPDATES", "$1,200/yr", ("OWNER_US_EQUITY_OPTIONS_PRICING_3",)),
    ("BULK_HOUR_UPDATES", "$1,440/yr", ("OWNER_US_EQUITY_OPTIONS_PRICING_3",)),
    ("BULK_DAILY_DOWNLOAD", "$12,000", ("OWNER_US_EQUITY_OPTIONS_PRICING_3",)),
    ("BULK_HOUR_DOWNLOAD", "$14,400", ("OWNER_US_EQUITY_OPTIONS_PRICING_4",)),
    ("BULK_MINUTE_DOWNLOAD", "$30,000", ("OWNER_US_EQUITY_OPTIONS_PRICING_4",)),
)

EXPECTED_AXIS_RECOMMENDATIONS: tuple[
    tuple[LicenseAssessmentAxis, OwnerReviewRecommendation], ...
] = (
    (
        LicenseAssessmentAxis.FREE_CLOUD_DATA_CLASS_ACCESS,
        OwnerReviewRecommendation.CONDITIONAL_GO_PUBLIC_FREE_AND_ACCOUNT_FREE,
    ),
    (
        LicenseAssessmentAxis.QQQ_OPTIONS_ACCOUNT_ENTITLEMENT,
        OwnerReviewRecommendation.CONDITIONAL_GO_TESTED_SESSION_ONLY,
    ),
    (
        LicenseAssessmentAxis.PRIMARY_WINDOW_HISTORICAL_RETENTION,
        OwnerReviewRecommendation.NO_GO_NOT_TESTED_ACCOUNT_SPECIFIC,
    ),
    (
        LicenseAssessmentAxis.RAW_OPTIONS_LOCAL_DOWNLOAD,
        OwnerReviewRecommendation.NO_GO_SEPARATE_PAID_DOWNLOAD_NOT_AUTHORIZED,
    ),
    (
        LicenseAssessmentAxis.RAW_OPTIONS_REDISTRIBUTION,
        OwnerReviewRecommendation.NO_GO_PROHIBITED,
    ),
    (
        LicenseAssessmentAxis.DERIVED_BACKTEST_RESULT_EXPORT,
        OwnerReviewRecommendation.NO_GO_PENDING_TRADING_2489,
    ),
    (
        LicenseAssessmentAxis.API_CLI_ACCESS,
        OwnerReviewRecommendation.NO_GO_FREE_TIER,
    ),
)

EXPECTED_OWNER_REVIEW_CHECKS: tuple[str, ...] = (
    "CONFIRM_ACTIVE_ORGANIZATION_FREE",
    "CONFIRM_FREE_PLAN_RESOURCE_LIMITS",
    "CONFIRM_US_EQUITY_OPTIONS_CLOUD_ACCESS_FREE",
    "CONFIRM_DOWNLOAD_PRICE_MAPPING_OBSERVATION_ONLY",
    "CONFIRM_NO_PURCHASE_SUBSCRIPTION_OR_DOWNLOAD",
    "CONFIRM_2021_02_22_PRIMARY_WINDOW_NOT_TESTED_ACCOUNT_SPECIFIC",
    "CONFIRM_2493_NO_GO_AND_2489_2490_BLOCKERS_REMAIN",
    "ACCEPT_OR_REJECT_HASH_BOUND_PROPOSAL",
)

PROPOSED_OWNER_DECISION = (
    "owner_decision:TRADING-2497:2026-08-07:"
    "accept_license_export_manual_review_keep_primary_window_and_shared_gates_blocked_v1"
)


class QCQQQOptionsLicenseExportOwnerReviewContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


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
        raise ValueError(f"{field} must be lowercase 40-character Git SHA")
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

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        expected = _canonical_sha256(self.semantic_payload())
        if self.content_sha256 not in {_UNSEALED_SHA256, expected}:
            raise ValueError("content_sha256 does not match canonical semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        candidate = cls.model_validate({**payload, "content_sha256": _UNSEALED_SHA256})
        return cls.model_validate(
            {**payload, "content_sha256": _canonical_sha256(candidate.semantic_payload())}
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("record is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("record JSON root must be an object")
        record = cls.model_validate(decoded, strict=False)
        if record.content_sha256 == _UNSEALED_SHA256:
            raise ValueError("record is unsealed")
        if record.canonical_bytes != raw:
            raise ValueError("record bytes are not canonical")
        return record


class OwnerManualEvidenceRecord(_PolicyModel):
    evidence_id: str
    evidence_kind: Literal["PNG", "PDF"]
    byte_count: int
    sha256: str
    role: OwnerManualEvidenceRole
    capture_method: Literal["OWNER_MANUAL_BROWSER_REVIEW"]
    retention_status: Literal["HASH_ONLY_EXTERNAL_FILE_NOT_RETAINED_IN_REPOSITORY"]
    reviewed_by: Literal["project_owner"]
    reviewed_on: date
    contains_raw_option_rows: Literal[False]
    observation_summary: str

    @field_validator("evidence_id")
    @classmethod
    def _validate_evidence_id(cls, value: str) -> str:
        return _identifier(value, "evidence_id")

    @field_validator("byte_count")
    @classmethod
    def _validate_byte_count(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("byte_count must be positive")
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")

    @field_validator("reviewed_on")
    @classmethod
    def _validate_review_date(cls, value: date) -> date:
        if value != date(2026, 8, 7):
            raise ValueError("reviewed_on must match the manual review date")
        return value

    @field_validator("observation_summary")
    @classmethod
    def _validate_summary(cls, value: str) -> str:
        return _required_text(value, "observation_summary")


class LicenseListingFact(_PolicyModel):
    listing_id: str
    display_value: str
    evidence_ids: tuple[str, ...]
    interpretation: Literal["OBSERVATION_ONLY_NOT_PURCHASE_OR_BUDGET_AUTHORITY"]

    @field_validator("listing_id")
    @classmethod
    def _validate_listing_id(cls, value: str) -> str:
        return _identifier(value, "listing_id")

    @field_validator("display_value")
    @classmethod
    def _validate_display_value(cls, value: str) -> str:
        return _required_text(value, "display_value")

    @field_validator("evidence_ids")
    @classmethod
    def _validate_evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("evidence_ids must be non-empty and unique")
        return tuple(_identifier(item, "evidence_id") for item in value)


class LicenseOwnerReviewAxisRecommendation(_PolicyModel):
    axis_id: LicenseAssessmentAxis
    recommendation: OwnerReviewRecommendation
    evidence_ids: tuple[str, ...]
    reason_code: str
    summary: str

    @field_validator("evidence_ids")
    @classmethod
    def _validate_evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("evidence_ids must be non-empty and unique")
        return tuple(_identifier(item, "evidence_id") for item in value)

    @field_validator("reason_code")
    @classmethod
    def _validate_reason_code(cls, value: str) -> str:
        return _identifier(value, "reason_code")

    @field_validator("summary")
    @classmethod
    def _validate_summary(cls, value: str) -> str:
        return _required_text(value, "summary")


class LicenseExportOwnerReviewSafety(_PolicyModel):
    proposal_only: Literal[True]
    owner_signature_present: Literal[False]
    external_platform_action_authorized: Literal[False]
    quantconnect_login_authorized: Literal[False]
    project_mutation_authorized: Literal[False]
    cloud_backtest_authorized: Literal[False]
    api_cli_http_object_store_authorized: Literal[False]
    raw_options_download_authorized: Literal[False]
    purchase_or_subscription_authorized: Literal[False]
    range_expansion_authorized: Literal[False]
    paid_tier_upgrade_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action: Literal["none"]


class QCQQQOptionsLicenseExportOwnerReviewPolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_license_export_owner_review_policy.v1"]
    policy_id: str
    policy_version: Literal["1.0.0"]
    status: Literal["PROPOSED_OWNER_REVIEW_REQUIRED"]
    effective_date: date
    predecessor_task_id: Literal["TRADING-2497_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_V1"]
    predecessor_report_relative_path: str
    predecessor_report_file_sha256: str
    predecessor_report_content_sha256: str
    predecessor_report_schema_version: Literal[
        "qc_qqq_options_license_export_due_diligence_report.v1"
    ]
    prepared_by: Literal["codex_license_evidence_coordinator"]
    reviewer: Literal["project_owner"]
    manual_evidence: tuple[OwnerManualEvidenceRecord, ...]
    listing_facts: tuple[LicenseListingFact, ...]
    axis_recommendations: tuple[LicenseOwnerReviewAxisRecommendation, ...]
    aggregate_recommendation: Literal["NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES"]
    proposed_owner_decision: Literal[
        "owner_decision:TRADING-2497:2026-08-07:accept_license_export_manual_review_keep_primary_window_and_shared_gates_blocked_v1"
    ]
    owner_review_checks: tuple[str, ...]
    primary_research_window_start: date
    tested_session: date
    safety: LicenseExportOwnerReviewSafety

    @field_validator("policy_id")
    @classmethod
    def _validate_policy_id(cls, value: str) -> str:
        return _identifier(value, "policy_id")

    @field_validator("predecessor_report_relative_path")
    @classmethod
    def _validate_predecessor_path(cls, value: str) -> str:
        checked = _required_text(value, "predecessor_report_relative_path")
        path = Path(checked)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != checked:
            raise ValueError("predecessor report path must be portable and repository-relative")
        return checked

    @field_validator("predecessor_report_file_sha256", "predecessor_report_content_sha256")
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("primary_research_window_start")
    @classmethod
    def _validate_primary_window(cls, value: date) -> date:
        if value != date(2021, 2, 22):
            raise ValueError("primary_research_window_start must remain 2021-02-22")
        return value

    @field_validator("tested_session")
    @classmethod
    def _validate_tested_session(cls, value: date) -> date:
        if value != date(2025, 12, 2):
            raise ValueError("tested_session must remain the accepted bounded pilot session")
        return value

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        evidence = tuple(
            (item.evidence_id, item.byte_count, item.sha256, item.role)
            for item in self.manual_evidence
        )
        if evidence != EXPECTED_EVIDENCE:
            raise ValueError("manual evidence inventory or identity drifted")
        facts = tuple(
            (item.listing_id, item.display_value, item.evidence_ids) for item in self.listing_facts
        )
        if facts != EXPECTED_LISTING_FACTS:
            raise ValueError("listing fact inventory or price mapping drifted")
        axis_pairs = tuple(
            (item.axis_id, item.recommendation) for item in self.axis_recommendations
        )
        if axis_pairs != EXPECTED_AXIS_RECOMMENDATIONS:
            raise ValueError("axis recommendation inventory or decision drifted")
        evidence_ids = {item.evidence_id for item in self.manual_evidence}
        for fact in self.listing_facts:
            if not set(fact.evidence_ids) <= evidence_ids:
                raise ValueError("listing fact references unknown manual evidence")
        for axis in self.axis_recommendations:
            if not set(axis.evidence_ids) <= evidence_ids:
                raise ValueError("axis recommendation references unknown manual evidence")
        if self.owner_review_checks != EXPECTED_OWNER_REVIEW_CHECKS:
            raise ValueError("owner review checks must remain exact and ordered")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QCQQQOptionsLicenseExportOwnerReviewPolicyLoadResult:
    policy: QCQQQOptionsLicenseExportOwnerReviewPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    evidence_set_sha256: str
    predecessor_report_path: Path


@dataclass(frozen=True)
class QCQQQOptionsLicenseExportOwnerReviewProposalLoadResult:
    proposal: QCQQQOptionsLicenseExportOwnerReviewProposal
    proposal_path: Path
    proposal_file_sha256: str
    policy: QCQQQOptionsLicenseExportOwnerReviewPolicyLoadResult


class QCQQQOptionsLicenseExportOwnerReviewProposal(_SealedModel):
    schema_version: Literal["qc_qqq_options_license_export_owner_review_proposal.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    evidence_set_sha256: str
    predecessor_report_file_sha256: str
    predecessor_report_content_sha256: str
    predecessor_aggregate_decision: Literal["LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED"]
    predecessor_owner_review_status: Literal["PENDING_MANUAL_OWNER_REVIEW"]
    manual_evidence: tuple[OwnerManualEvidenceRecord, ...]
    listing_facts: tuple[LicenseListingFact, ...]
    axis_recommendations: tuple[LicenseOwnerReviewAxisRecommendation, ...]
    aggregate_recommendation: Literal["NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES"]
    proposed_owner_decision: Literal[
        "owner_decision:TRADING-2497:2026-08-07:accept_license_export_manual_review_keep_primary_window_and_shared_gates_blocked_v1"
    ]
    owner_review_checks: tuple[str, ...]
    primary_research_window_start: date
    tested_session: date
    primary_window_status: Literal["NOT_TESTED_ACCOUNT_SPECIFIC"]
    owner_review_completed: Literal[False]
    owner_attestation_present: Literal[False]
    legal_opinion_provided: Literal[False]
    safety: LicenseExportOwnerReviewSafety

    @field_validator("record_id", "policy_id")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        checked = _utc(value, "created_at_utc")
        if checked.date() != date(2026, 8, 7):
            raise ValueError("created_at_utc must match the proposal date")
        return checked

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "evidence_set_sha256",
        "predecessor_report_file_sha256",
        "predecessor_report_content_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("primary_research_window_start")
    @classmethod
    def _validate_primary_window(cls, value: date) -> date:
        if value != date(2021, 2, 22):
            raise ValueError("primary research window must remain 2021-02-22")
        return value

    @field_validator("tested_session")
    @classmethod
    def _validate_tested_session(cls, value: date) -> date:
        if value != date(2025, 12, 2):
            raise ValueError("tested_session must remain 2025-12-02")
        return value

    @model_validator(mode="after")
    def _validate_proposal(self) -> Self:
        evidence = tuple(
            (item.evidence_id, item.byte_count, item.sha256, item.role)
            for item in self.manual_evidence
        )
        facts = tuple(
            (item.listing_id, item.display_value, item.evidence_ids) for item in self.listing_facts
        )
        axis_pairs = tuple(
            (item.axis_id, item.recommendation) for item in self.axis_recommendations
        )
        if evidence != EXPECTED_EVIDENCE:
            raise ValueError("proposal manual evidence inventory drifted")
        if facts != EXPECTED_LISTING_FACTS:
            raise ValueError("proposal listing fact inventory drifted")
        if axis_pairs != EXPECTED_AXIS_RECOMMENDATIONS:
            raise ValueError("proposal axis recommendations drifted")
        if self.owner_review_checks != EXPECTED_OWNER_REVIEW_CHECKS:
            raise ValueError("proposal owner review checks drifted")
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


def load_qc_qqq_options_license_export_owner_review_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsLicenseExportOwnerReviewPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="license/export Owner-review policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("license/export Owner-review policy root must be a mapping")
        policy = QCQQQOptionsLicenseExportOwnerReviewPolicy.model_validate(payload)
        predecessor_path = _require_bound_regular_file(
            Path(policy.predecessor_report_relative_path),
            project_root=root,
            field="TRADING-2497 predecessor report",
        )
        predecessor_raw = predecessor_path.read_bytes()
        if hashlib.sha256(predecessor_raw).hexdigest() != policy.predecessor_report_file_sha256:
            raise ValueError("TRADING-2497 predecessor report file SHA-256 mismatch")
        predecessor = QCQQQOptionsLicenseExportDueDiligenceReport.from_json_bytes(predecessor_raw)
        if predecessor.content_sha256 != policy.predecessor_report_content_sha256:
            raise ValueError("TRADING-2497 predecessor report content SHA-256 mismatch")
        if (
            predecessor.aggregate_decision != "LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED"
            or predecessor.owner_review_status != "PENDING_MANUAL_OWNER_REVIEW"
            or predecessor.primary_research_window_start != date(2021, 2, 22)
            or predecessor.research_run_performed
            or predecessor.requested_range is not None
            or predecessor.evaluated_range is not None
        ):
            raise ValueError("TRADING-2497 predecessor safety or research boundary drifted")
    except QCQQQOptionsLicenseExportOwnerReviewContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsLicenseExportOwnerReviewContractError(
            "QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsLicenseExportOwnerReviewPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        evidence_set_sha256=_canonical_sha256(
            [item.model_dump(mode="json") for item in policy.manual_evidence]
        ),
        predecessor_report_path=predecessor_path,
    )


def build_qc_qqq_options_license_export_owner_review_proposal(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsLicenseExportOwnerReviewProposal:
    loaded = load_qc_qqq_options_license_export_owner_review_policy(
        policy_path, project_root=project_root
    )
    predecessor_raw = loaded.predecessor_report_path.read_bytes()
    predecessor = QCQQQOptionsLicenseExportDueDiligenceReport.from_json_bytes(predecessor_raw)
    policy = loaded.policy
    return QCQQQOptionsLicenseExportOwnerReviewProposal.seal(
        schema_version="qc_qqq_options_license_export_owner_review_proposal.v1",
        record_id=record_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        evidence_set_sha256=loaded.evidence_set_sha256,
        predecessor_report_file_sha256=hashlib.sha256(predecessor_raw).hexdigest(),
        predecessor_report_content_sha256=predecessor.content_sha256,
        predecessor_aggregate_decision="LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED",
        predecessor_owner_review_status="PENDING_MANUAL_OWNER_REVIEW",
        manual_evidence=policy.manual_evidence,
        listing_facts=policy.listing_facts,
        axis_recommendations=policy.axis_recommendations,
        aggregate_recommendation=policy.aggregate_recommendation,
        proposed_owner_decision=policy.proposed_owner_decision,
        owner_review_checks=policy.owner_review_checks,
        primary_research_window_start=policy.primary_research_window_start,
        tested_session=policy.tested_session,
        primary_window_status="NOT_TESTED_ACCOUNT_SPECIFIC",
        owner_review_completed=False,
        owner_attestation_present=False,
        legal_opinion_provided=False,
        safety=policy.safety,
    )


def load_qc_qqq_options_license_export_owner_review_proposal(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_PROPOSAL_PATH,
    *,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsLicenseExportOwnerReviewProposalLoadResult:
    root = project_root.resolve()
    try:
        proposal_path = _require_bound_regular_file(
            path, project_root=root, field="license/export Owner-review proposal"
        )
        raw = proposal_path.read_bytes()
        proposal = QCQQQOptionsLicenseExportOwnerReviewProposal.from_json_bytes(raw)
        expected = build_qc_qqq_options_license_export_owner_review_proposal(
            record_id=proposal.record_id,
            created_at_utc=proposal.created_at_utc,
            repository_code_sha=proposal.repository_code_sha,
            policy_path=policy_path,
            project_root=root,
        )
        if proposal != expected:
            raise ValueError("proposal does not replay from current frozen policy authority")
        policy = load_qc_qqq_options_license_export_owner_review_policy(
            policy_path, project_root=root
        )
    except QCQQQOptionsLicenseExportOwnerReviewContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsLicenseExportOwnerReviewContractError(
            "QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_PROPOSAL_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsLicenseExportOwnerReviewProposalLoadResult(
        proposal=proposal,
        proposal_path=proposal_path,
        proposal_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy=policy,
    )
