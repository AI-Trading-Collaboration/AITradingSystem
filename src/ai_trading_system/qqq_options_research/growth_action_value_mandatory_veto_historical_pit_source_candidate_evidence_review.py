from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_historical_pit_static_authority_receipt_contract as s11,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_HISTORICAL_PIT_SOURCE_CANDIDATE_EVIDENCE_REVIEW_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "historical_pit_source_candidate_evidence_review_v1.yaml"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL: Literal["S12_OWNER_APPROVAL_PACKET_READY_EXACT_HISTORICAL_COVERAGE_UNPROVEN_0_OF_5"] = (
    "S12_OWNER_APPROVAL_PACKET_READY_EXACT_HISTORICAL_COVERAGE_UNPROVEN_0_OF_5"
)
_PRIMARY_WINDOW = (
    "XNYS",
    date(2021, 2, 22),
    date(2025, 12, 2),
    1202,
    "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0",
)
_SOURCE_REVIEW_SURFACE = (
    (
        "fmp_spy_qqq_eod_adjusted_v1",
        "VENDOR_EVIDENCE_REQUIRED",
        False,
        "VENDOR_PRODUCT_DOCUMENTATION_ONLY",
        "PUBLISHED_SELF_SERVE_REFERENCE_NOT_REQUIRED_SCOPE_QUOTE",
        "KEEP_VENDOR_EVIDENCE_REQUIRED",
    ),
    (
        "cboe_vix_index_daily_v1",
        "VENDOR_EVIDENCE_REQUIRED",
        False,
        "VENDOR_PRODUCT_DOCUMENTATION_ONLY",
        "PRODUCT_QUOTE_UNKNOWN_ALL_ACCESS_REFERENCE_NOT_SUBSTITUTE",
        "KEEP_VENDOR_EVIDENCE_REQUIRED",
    ),
    (
        "federal_reserve_fomc_schedule_capture_v1",
        "FREEZE_CANDIDATE",
        True,
        "OFFICIAL_INITIAL_SCHEDULE_FAMILY_AND_FINAL_CALENDAR_LOCATED",
        "PUBLIC_OFFICIAL_NO_ACCESS_FEE_IDENTIFIED",
        "RETAIN_FROZEN_CANDIDATE_FAMILY",
    ),
    (
        "bls_release_schedule_capture_v1",
        "INVENTORY_ONLY",
        False,
        "OFFICIAL_ANNUAL_ARCHIVE_AND_RESULT_RELEASE_ARCHIVES_LOCATED",
        "PUBLIC_OFFICIAL_NO_ACCESS_FEE_IDENTIFIED",
        "PROMOTE_TO_OWNER_FREEZE_CANDIDATE_AFTER_EXACT_DOCUMENT_INVENTORY",
    ),
    (
        "bea_release_schedule_capture_v1",
        "INVENTORY_ONLY",
        False,
        "OFFICIAL_ANNUAL_PDF_SCB_SCHEDULE_AND_UPDATE_NOTICE_FAMILY_LOCATED",
        "PUBLIC_OFFICIAL_NO_ACCESS_FEE_IDENTIFIED",
        "PROMOTE_TO_OWNER_FREEZE_CANDIDATE_AFTER_EXACT_DOCUMENT_INVENTORY",
    ),
)
_OWNER_DECISION_SURFACE = (
    (
        "APPROVE_VENDOR_EVIDENCE_INQUIRY_SEND",
        ("fmp_spy_qqq_eod_adjusted_v1", "cboe_vix_index_daily_v1"),
        "R2_MATERIAL_EXTERNAL_CHANGE",
    ),
    (
        "APPROVE_OFFICIAL_DOCUMENT_EXACT_FREEZE_INVENTORY",
        (
            "federal_reserve_fomc_schedule_capture_v1",
            "bls_release_schedule_capture_v1",
            "bea_release_schedule_capture_v1",
        ),
        "R1_BOUNDED_RESEARCH_SANDBOX",
    ),
)


class HistoricalPITSourceCandidateEvidenceReviewError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


class _CanonicalModel(_StrictModel):
    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes project root")
    current = resolved_root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    return resolved


class OwnerScope(_StrictModel):
    instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:S12:2026-08-28:continue_read_only_five_source_discovery"
    ]
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH_SOURCE_CANDIDATE_REVIEW_ONLY"]
    public_documentation_discovery_allowed: Literal[True]
    vendor_contact_allowed: Literal[False]
    paid_product_purchase_allowed: Literal[False]
    provider_api_query_allowed: Literal[False]
    official_document_payload_download_allowed: Literal[False]
    real_market_payload_download_allowed: Literal[False]


class PredecessorBinding(_StrictModel):
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_static_authority_receipt_contract_v1"
    ]
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_static_authority_receipt_contract_v1.yaml"
    ]
    file_sha256: str
    canonical_sha256: str
    role: Literal["S11_STATIC_AUTHORITY_RECEIPT_CONTRACT"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class PrimaryResearchWindow(_StrictModel):
    exchange_calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    exact_session_count: Literal[1202]
    ordered_session_ids_lf_sha256: str
    primary_window_change_allowed: Literal[False]

    @field_validator("ordered_session_ids_lf_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class PublicEvidenceLocator(_StrictModel):
    evidence_id: str
    url: str
    source_role: Literal[
        "VENDOR_PRODUCT_DOCUMENTATION",
        "VENDOR_PRICING_REFERENCE",
        "OFFICIAL_INITIAL_SCHEDULE",
        "OFFICIAL_FINAL_OR_ARCHIVED_SCHEDULE",
        "OFFICIAL_REVISION_NOTICE_EXAMPLE",
        "OFFICIAL_RESULT_RELEASE_ARCHIVE",
        "OFFICIAL_PUBLICATION_POLICY",
    ]
    observed_claim: str
    discovery_as_of: date
    exact_remote_bytes_captured: Literal[False]
    remote_payload_sha256: Literal[None]

    @model_validator(mode="after")
    def validate_locator(self) -> Self:
        if not self.url.startswith("https://"):
            raise ValueError("public evidence locator must use HTTPS")
        if not self.evidence_id or not self.observed_claim:
            raise ValueError("public evidence locator text cannot be empty")
        return self


class CoverageAssessment(_StrictModel):
    evidence_class: Literal[
        "VENDOR_PRODUCT_DOCUMENTATION_ONLY",
        "OFFICIAL_INITIAL_SCHEDULE_FAMILY_AND_FINAL_CALENDAR_LOCATED",
        "OFFICIAL_ANNUAL_ARCHIVE_AND_RESULT_RELEASE_ARCHIVES_LOCATED",
        "OFFICIAL_ANNUAL_PDF_SCB_SCHEDULE_AND_UPDATE_NOTICE_FAMILY_LOCATED",
    ]
    nominal_scope: str
    nominal_2021_2025_scope_located: bool
    exact_remote_bytes_and_digests_frozen: Literal[False]
    per_row_historical_available_at_proven: Literal[False]
    complete_revision_or_reissue_ledger_proven: Literal[False]
    exact_1202_cutoff_coverage_proven: Literal[False]


class FeeEvidence(_StrictModel):
    fee_state: Literal[
        "PUBLISHED_SELF_SERVE_REFERENCE_NOT_REQUIRED_SCOPE_QUOTE",
        "PRODUCT_QUOTE_UNKNOWN_ALL_ACCESS_REFERENCE_NOT_SUBSTITUTE",
        "PUBLIC_OFFICIAL_NO_ACCESS_FEE_IDENTIFIED",
    ]
    published_reference_points: tuple[str, ...]
    exact_required_scope_quote_obtained: Literal[False]
    internal_research_license_rights_proven: bool
    purchase_authorized: Literal[False]

    @model_validator(mode="after")
    def validate_fee_evidence(self) -> Self:
        if not self.published_reference_points:
            raise ValueError("fee evidence must retain at least one reference point")
        return self


Recommendation = Literal[
    "KEEP_VENDOR_EVIDENCE_REQUIRED",
    "RETAIN_FROZEN_CANDIDATE_FAMILY",
    "PROMOTE_TO_OWNER_FREEZE_CANDIDATE_AFTER_EXACT_DOCUMENT_INVENTORY",
]


class SourceReviewRow(_StrictModel):
    ordinal: int
    candidate_id: str
    provider_or_authority: str
    predecessor_candidate_disposition: Literal[
        "FREEZE_CANDIDATE", "INVENTORY_ONLY", "VENDOR_EVIDENCE_REQUIRED"
    ]
    predecessor_candidate_source_approved: bool
    public_evidence_locators: tuple[PublicEvidenceLocator, ...]
    coverage_assessment: CoverageAssessment
    fee_evidence: FeeEvidence
    key_gaps: tuple[str, ...]
    recommendation: Recommendation
    recommendation_requires_owner_approval: Literal[True]
    candidate_source_approved_after_review: bool
    exact_authority_identity: Literal[None]
    exact_authority_identity_frozen: Literal[False]
    historical_coverage_proven: Literal[False]
    source_contract_admitted: Literal[False]
    runtime_authorized: Literal[False]
    blocker_remediated: Literal[False]

    @model_validator(mode="after")
    def validate_source_review_row(self) -> Self:
        if not self.public_evidence_locators:
            raise ValueError("source row must cite public discovery evidence")
        if not self.key_gaps or len(set(self.key_gaps)) != len(self.key_gaps):
            raise ValueError("source row gaps must be non-empty and unique")
        if self.candidate_source_approved_after_review != (
            self.predecessor_candidate_source_approved
        ):
            raise ValueError("S12 cannot change candidate source approval state")
        return self


class OwnerDecisionRequest(_StrictModel):
    request_id: Literal[
        "APPROVE_VENDOR_EVIDENCE_INQUIRY_SEND",
        "APPROVE_OFFICIAL_DOCUMENT_EXACT_FREEZE_INVENTORY",
    ]
    candidate_ids: tuple[str, ...]
    risk_tier: Literal["R1_BOUNDED_RESEARCH_SANDBOX", "R2_MATERIAL_EXTERNAL_CHANGE"]
    requested_action: str
    approval_granted_in_this_artifact: Literal[False]
    automatic_source_state_change_allowed: Literal[False]


class Aggregate(_StrictModel):
    source_row_count: Literal[5]
    candidate_source_approved_count: Literal[1]
    exact_authority_identity_frozen_count: Literal[0]
    historical_coverage_proven_count: Literal[0]
    source_contract_admitted_count: Literal[0]
    runtime_authorized_count: Literal[0]
    remediated_blocker_count: Literal[0]
    owner_decision_request_count: Literal[2]
    next_legal_action: Literal["OWNER_REVIEWS_VENDOR_INQUIRY_AND_OFFICIAL_DOCUMENT_FREEZE_REQUESTS"]
    terminal: Literal["S12_OWNER_APPROVAL_PACKET_READY_EXACT_HISTORICAL_COVERAGE_UNPROVEN_0_OF_5"]


class ObservedExternalActivity(_StrictModel):
    public_documentation_web_discovery_performed: Literal[True]
    exact_browser_http_request_count_recorded: Literal[False]
    exact_browser_http_request_count: Literal[None]
    provider_api_query_attempt_count: Literal[0]
    vendor_contact_count: Literal[0]
    paid_product_purchase_count: Literal[0]
    official_document_payload_download_count: Literal[0]
    real_market_payload_download_count: Literal[0]
    cache_read_count: Literal[0]
    market_data_file_read_count: Literal[0]
    source_inventory_admission_count: Literal[0]
    veto_series_generation_count: Literal[0]
    real_dq_run_count: Literal[0]
    backtest_run_count: Literal[0]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    production_effect_count: Literal[0]
    broker_action_count: Literal[0]


class Safety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    public_documentation_web_discovery_allowed: Literal[True]
    provider_api_query_allowed: Literal[False]
    vendor_contact_allowed: Literal[False]
    paid_product_purchase_allowed: Literal[False]
    official_document_payload_download_allowed: Literal[False]
    real_market_payload_download_allowed: Literal[False]
    source_inventory_admission_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    real_dq_allowed: Literal[False]
    backtest_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class HistoricalPITSourceCandidateEvidenceReview(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_source_candidate_evidence_review_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["OWNER_REVIEW_PACKET_READY_NO_AUTHORITY_ADMISSION"]
    task_id: Literal["TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"]
    discovery_as_of: date
    owner_scope: OwnerScope
    predecessor_binding: PredecessorBinding
    primary_research_window: PrimaryResearchWindow
    source_rows: tuple[SourceReviewRow, ...]
    owner_decision_requests: tuple[OwnerDecisionRequest, ...]
    aggregate: Aggregate
    observed_external_activity: ObservedExternalActivity
    safety: Safety

    @model_validator(mode="after")
    def validate_review_surface(self) -> Self:
        observed_window = (
            self.primary_research_window.exchange_calendar,
            self.primary_research_window.requested_start,
            self.primary_research_window.requested_end,
            self.primary_research_window.exact_session_count,
            self.primary_research_window.ordered_session_ids_lf_sha256,
        )
        if observed_window != _PRIMARY_WINDOW:
            raise ValueError("primary research window drifted")
        observed_sources = tuple(
            (
                row.candidate_id,
                row.predecessor_candidate_disposition,
                row.predecessor_candidate_source_approved,
                row.coverage_assessment.evidence_class,
                row.fee_evidence.fee_state,
                row.recommendation,
            )
            for row in self.source_rows
        )
        if observed_sources != _SOURCE_REVIEW_SURFACE:
            raise ValueError("five-source evidence review surface drifted")
        if tuple(row.ordinal for row in self.source_rows) != tuple(range(1, 6)):
            raise ValueError("five-source order drifted")
        observed_decisions = tuple(
            (row.request_id, row.candidate_ids, row.risk_tier)
            for row in self.owner_decision_requests
        )
        if observed_decisions != _OWNER_DECISION_SURFACE:
            raise ValueError("owner decision surface drifted")
        approved = sum(row.candidate_source_approved_after_review for row in self.source_rows)
        if approved != self.aggregate.candidate_source_approved_count:
            raise ValueError("candidate approval aggregate drifted")
        return self


@dataclass(frozen=True)
class HistoricalPITSourceCandidateEvidenceReviewLoadResult:
    policy: HistoricalPITSourceCandidateEvidenceReview
    path: Path
    file_sha256: str
    canonical_sha256: str
    s11_static_contract: s11.HistoricalPITStaticAuthorityReceiptContractLoadResult
    terminal: Literal["S12_OWNER_APPROVAL_PACKET_READY_EXACT_HISTORICAL_COVERAGE_UNPROVEN_0_OF_5"]


def load_historical_pit_source_candidate_evidence_review(
    *,
    path: Path = DEFAULT_HISTORICAL_PIT_SOURCE_CANDIDATE_EVIDENCE_REVIEW_PATH,
    project_root: Path = PROJECT_ROOT,
) -> HistoricalPITSourceCandidateEvidenceReviewLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="s12_evidence_review")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = HistoricalPITSourceCandidateEvidenceReview.model_validate(payload)
        loaded_s11 = s11.load_historical_pit_static_authority_receipt_contract(
            project_root=project_root
        )
        binding = policy.predecessor_binding
        bound_s11 = _bound_file(Path(binding.path), root=project_root, field="s11_predecessor")
        if hashlib.sha256(bound_s11.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S11 predecessor file SHA-256 mismatch")
        if (binding.file_sha256, binding.canonical_sha256) != (
            loaded_s11.file_sha256,
            loaded_s11.canonical_sha256,
        ):
            raise ValueError("S11 predecessor exact identity drifted")

        predecessor_surface = tuple(
            (
                row.candidate_id,
                row.candidate_disposition,
                row.candidate_source_approved,
            )
            for row in loaded_s11.policy.source_rows
        )
        review_surface = tuple(
            (
                row.candidate_id,
                row.predecessor_candidate_disposition,
                row.predecessor_candidate_source_approved,
            )
            for row in policy.source_rows
        )
        if review_surface != predecessor_surface:
            raise ValueError("S12 does not replay the exact S11 source state")

        predecessor_window = loaded_s11.policy.primary_research_window
        expected_window = (
            predecessor_window.exchange_calendar,
            predecessor_window.requested_start,
            predecessor_window.requested_end,
            predecessor_window.exact_session_count,
            predecessor_window.ordered_session_ids_lf_sha256,
        )
        observed_window = (
            policy.primary_research_window.exchange_calendar,
            policy.primary_research_window.requested_start,
            policy.primary_research_window.requested_end,
            policy.primary_research_window.exact_session_count,
            policy.primary_research_window.ordered_session_ids_lf_sha256,
        )
        if observed_window != expected_window:
            raise ValueError("S12 primary research window drifted from S11")
    except HistoricalPITSourceCandidateEvidenceReviewError:
        raise
    except s11.HistoricalPITStaticAuthorityReceiptContractError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise HistoricalPITSourceCandidateEvidenceReviewError(
            "HISTORICAL_PIT_SOURCE_CANDIDATE_EVIDENCE_REVIEW_REJECTED", str(exc)
        ) from exc
    return HistoricalPITSourceCandidateEvidenceReviewLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        s11_static_contract=loaded_s11,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_HISTORICAL_PIT_SOURCE_CANDIDATE_EVIDENCE_REVIEW_PATH",
    "HistoricalPITSourceCandidateEvidenceReview",
    "HistoricalPITSourceCandidateEvidenceReviewError",
    "HistoricalPITSourceCandidateEvidenceReviewLoadResult",
    "load_historical_pit_source_candidate_evidence_review",
]
