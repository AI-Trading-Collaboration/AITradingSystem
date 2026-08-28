from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review as s12,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_HISTORICAL_PIT_SOURCE_EVIDENCE_ACQUISITION_RECEIPT_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "historical_pit_source_evidence_acquisition_receipt_v1.yaml"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL: Literal[
    "S13_EVIDENCE_RECEIPTS_READY_VENDOR_SEND_AND_BLS_EXACT_BYTES_BLOCKED"
] = "S13_EVIDENCE_RECEIPTS_READY_VENDOR_SEND_AND_BLS_EXACT_BYTES_BLOCKED"
_APPROVAL_SURFACE = (
    (
        "APPROVE_VENDOR_EVIDENCE_INQUIRY_SEND",
        "R2_MATERIAL_EXTERNAL_CHANGE",
        "PREPARE_AND_ATTEMPT_FMP_CBOE_CAPABILITY_LICENSE_QUOTE_INQUIRIES_"
        "NO_PURCHASE_NO_PROVIDER_API",
    ),
    (
        "APPROVE_OFFICIAL_DOCUMENT_EXACT_FREEZE_INVENTORY",
        "R1_BOUNDED_RESEARCH_SANDBOX",
        "DOWNLOAD_HASH_AND_REVIEW_FED_BLS_BEA_OFFICIAL_SCHEDULE_METADATA_ONLY",
    ),
)
_VENDOR_SURFACE = (
    (
        "FMP",
        "fmp_spy_qqq_eod_adjusted_v1",
        "SEND_BLOCKED_MISSING_AUTHORIZED_SENDER_IDENTITY_OR_CHANNEL",
    ),
    (
        "CBOE_DATASHOP",
        "cboe_vix_index_daily_v1",
        "SEND_BLOCKED_MISSING_AUTHORIZED_SENDER_IDENTITY_OR_CHANNEL_AND_CAPTCHA_CONFIRMATION",
    ),
)
_RECEIPT_IDS = (
    "fed_fomc_2021_tentative_schedule_press_release",
    "fed_fomc_2022_tentative_schedule_press_release",
    "fed_fomc_2023_tentative_schedule_press_release",
    "fed_fomc_2024_tentative_schedule_press_release",
    "fed_fomc_2025_2026_tentative_schedule_press_release",
    "fed_current_and_historical_fomc_calendars",
    "bea_2021_annual_schedule_pdf",
    "bea_2022_annual_schedule_pdf",
    "bea_2023_annual_schedule_pdf",
    "bea_2024_annual_schedule_pdf",
    "bea_2025_annual_schedule_pdf",
    "bea_2023_scb_schedule_page",
    "bea_news_archive",
    "bea_schedule_publication_policy",
    "bea_2023_benchmark_reschedule_notice",
    "bea_2021_scb_schedule_page_same_family",
    "bea_2022_scb_schedule_page_same_family",
    "bea_2024_scb_schedule_page_same_family",
)
_FAILED_IDS = (
    "bls_archived_release_schedule_index",
    "bls_archived_release_schedule_index_browser_profile_retry",
    "bls_2021_release_schedule",
    "bls_2022_release_schedule",
    "bls_2023_release_schedule_same_family",
    "bls_2024_release_schedule",
    "bls_2025_release_schedule_same_family",
    "bls_current_cpi_schedule",
    "bls_archived_cpi_releases",
    "bls_archived_employment_situation_releases",
    "bea_2021_annual_schedule_pdf_stale_s12_locator",
    "bea_2022_annual_schedule_pdf_stale_s12_locator",
    "bea_2023_annual_schedule_pdf_stale_s12_locator",
    "bea_2024_annual_schedule_pdf_stale_s12_locator",
    "bea_2025_annual_schedule_pdf_stale_s12_locator",
)


class HistoricalPITSourceEvidenceAcquisitionReceiptError(ValueError):
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
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    resolved.relative_to(resolved_root)
    return resolved


def _validate_sha(value: str) -> str:
    if not _SHA256.fullmatch(value):
        raise ValueError("invalid lowercase SHA-256")
    return value


class PredecessorBinding(_StrictModel):
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_source_candidate_evidence_review_v1"
    ]
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_source_candidate_evidence_review_v1.yaml"
    ]
    file_sha256: str
    canonical_sha256: str
    role: Literal["S12_SOURCE_CANDIDATE_EVIDENCE_REVIEW"]
    immutable: Literal[True]

    _sha = field_validator("file_sha256", "canonical_sha256")(_validate_sha)


class OwnerApproval(_StrictModel):
    ordinal: int
    request_id: Literal[
        "APPROVE_VENDOR_EVIDENCE_INQUIRY_SEND",
        "APPROVE_OFFICIAL_DOCUMENT_EXACT_FREEZE_INVENTORY",
    ]
    risk_tier: Literal["R1_BOUNDED_RESEARCH_SANDBOX", "R2_MATERIAL_EXTERNAL_CHANGE"]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]
    owner_instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:S13:2026-08-28:approve_both_s12_requests"
    ]
    approval_scope: str
    automatic_technical_state_change_allowed: Literal[False]


class VendorRow(_StrictModel):
    ordinal: int
    vendor: Literal["FMP", "CBOE_DATASHOP"]
    candidate_id: str
    official_contact_url: str
    official_contact_email_observed: str | None
    contact_surface_inspected: Literal[True]
    required_identity_fields: tuple[str, ...]
    captcha_observed: bool
    browser_action_time_confirmation_required: Literal[True]
    form_submission_attempted: Literal[False]
    message_sent: Literal[False]
    sent_receipt: Literal[None]
    send_state: Literal[
        "SEND_BLOCKED_MISSING_AUTHORIZED_SENDER_IDENTITY_OR_CHANNEL",
        "SEND_BLOCKED_MISSING_AUTHORIZED_SENDER_IDENTITY_OR_CHANNEL_AND_CAPTCHA_CONFIRMATION",
    ]

    @model_validator(mode="after")
    def validate_row(self) -> Self:
        if not self.official_contact_url.startswith("https://"):
            raise ValueError("vendor contact URL must use HTTPS")
        if not self.required_identity_fields:
            raise ValueError("vendor sender identity requirements cannot be empty")
        return self


class VendorInquiryPacket(_StrictModel):
    path: str
    file_sha256: str
    byte_count: int
    packet_state: Literal["PREPARED_NOT_SENT"]
    vendor_rows: tuple[VendorRow, ...]

    _sha = field_validator("file_sha256")(_validate_sha)


class OfficialDocumentReceipt(_StrictModel):
    ordinal: int
    authority: Literal["FED", "BEA"]
    document_id: str
    source_role: Literal[
        "OFFICIAL_INITIAL_SCHEDULE",
        "OFFICIAL_FINAL_OR_ARCHIVED_SCHEDULE",
        "OFFICIAL_REVISION_NOTICE_EXAMPLE",
        "OFFICIAL_RESULT_RELEASE_ARCHIVE",
        "OFFICIAL_PUBLICATION_POLICY",
    ]
    locator_disposition: Literal[
        "S12_EXACT_LOCATOR",
        "S12_LOCATOR_STALE_CORRECTED_OFFICIAL_PATH",
        "S13_SAME_FAMILY_ADDITION",
    ]
    requested_url: str
    retrieved_at: datetime
    http_status: Literal[200]
    content_type: str
    response_last_modified: str | None
    response_etag: str | None
    byte_count: int
    sha256: str
    retained_path: str

    _sha = field_validator("sha256")(_validate_sha)

    @model_validator(mode="after")
    def validate_receipt(self) -> Self:
        if not self.requested_url.startswith("https://"):
            raise ValueError("official document URL must use HTTPS")
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("retrieval timestamp must be timezone-aware")
        if self.byte_count <= 0 or not self.content_type:
            raise ValueError("retained document metadata is incomplete")
        return self


class FailedOfficialDocumentAttempt(_StrictModel):
    ordinal: int
    authority: Literal["BLS", "BEA"]
    document_id: str
    requested_url: str
    retrieved_at: datetime
    transport_profile: Literal[
        "CONTROLLED_METADATA_USER_AGENT", "BROWSER_LIKE_USER_AGENT_RETRY"
    ]
    curl_exit: Literal[0]
    http_status: Literal[403, 404]
    content_type: str
    response_byte_count: int
    retained_as_authority: Literal[False]
    reason_code: Literal[
        "BLS_AUTOMATED_EXACT_BYTE_DOWNLOAD_FORBIDDEN_HTTP_403",
        "BEA_S12_SYSTEM_FILES_LOCATOR_STALE_HTTP_404",
    ]

    @model_validator(mode="after")
    def validate_attempt(self) -> Self:
        if not self.requested_url.startswith("https://"):
            raise ValueError("failed official document URL must use HTTPS")
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("retrieval timestamp must be timezone-aware")
        if self.response_byte_count <= 0:
            raise ValueError("failed response byte count must be positive")
        expected = (self.authority, self.http_status, self.reason_code)
        allowed = {
            (
                "BLS",
                403,
                "BLS_AUTOMATED_EXACT_BYTE_DOWNLOAD_FORBIDDEN_HTTP_403",
            ),
            ("BEA", 404, "BEA_S12_SYSTEM_FILES_LOCATOR_STALE_HTTP_404"),
        }
        if expected not in allowed:
            raise ValueError("failed request authority/status/reason mismatch")
        return self


class RevisionGapAssessment(_StrictModel):
    ordinal: int
    authority: Literal["FED", "BLS", "BEA"]
    exact_document_receipt_count: int
    nominal_2021_2025_scope_located: Literal[True]
    exact_current_bytes_retained: bool
    initial_schedule_family_evidence_present: bool
    complete_revision_or_reissue_ledger_proven: Literal[False]
    historical_available_at_proven: Literal[False]
    exact_1202_cutoff_coverage_proven: Literal[False]
    blocker: str


class SourceState(_StrictModel):
    source_row_count: Literal[5]
    candidate_source_approved_count: Literal[1]
    exact_authority_identity_frozen_count: Literal[0]
    historical_coverage_proven_count: Literal[0]
    source_contract_admitted_count: Literal[0]
    runtime_authorized_count: Literal[0]
    remediated_blocker_count: Literal[0]


class ObservedExternalActivity(_StrictModel):
    browser_contact_surface_inspection_performed: Literal[True]
    exact_browser_http_request_count_recorded: Literal[False]
    exact_browser_http_request_count: Literal[None]
    vendor_inquiry_packet_prepared_count: Literal[2]
    vendor_form_submission_attempt_count: Literal[0]
    vendor_contact_count: Literal[0]
    provider_api_query_attempt_count: Literal[0]
    official_metadata_http_request_attempt_count: Literal[33]
    official_document_exact_bytes_retained_count: Literal[18]
    official_document_failed_http_attempt_count: Literal[15]
    official_metadata_file_read_for_validation_count: Literal[18]
    paid_product_purchase_count: Literal[0]
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


class TemporaryWorkspace(_StrictModel):
    path: Literal["D:/Work/AITradingSystem/.tmp/trading-2542g-s13-official-metadata"]
    owner_task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    purpose: Literal["BYTE_EXACT_HTTP_STAGING_AND_FAILURE_HEADER_CAPTURE"]
    unique_evidence_remaining_after_canonical_receipt_write: Literal[False]
    active_process_dependency: Literal[False]
    cleanup_authorized_after_validation: Literal[True]
    cleanup_completed: Literal[True]
    removed_file_count: Literal[43]
    released_byte_count: Literal[36968]
    removed_staging_recoverable: Literal[False]
    retained_destination: Literal[
        "inputs/research/qqq_options/trading_2542g_s13_source_evidence_acquisition_v1"
    ]
    recoverability: Literal[
        "VERIFIED_SUCCESS_BYTES_AND_RECEIPTS_RETAINED_IN_GIT_FAILURE_BODIES_DISPOSABLE"
    ]


class Safety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    vendor_contact_allowed_with_exact_scope: Literal[True]
    official_schedule_metadata_download_allowed: Literal[True]
    provider_api_query_allowed: Literal[False]
    paid_product_purchase_allowed: Literal[False]
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


class Aggregate(_StrictModel):
    owner_approval_count: Literal[2]
    vendor_packet_ready_count: Literal[2]
    vendor_message_sent_count: Literal[0]
    official_document_receipt_count: Literal[18]
    official_authority_with_any_exact_receipt_count: Literal[2]
    official_authority_with_complete_historical_coverage_count: Literal[0]
    exact_authority_identity_frozen_count: Literal[0]
    historical_coverage_proven_count: Literal[0]
    source_contract_admitted_count: Literal[0]
    runtime_authorized_count: Literal[0]
    blocker_remediated_count: Literal[0]
    next_legal_action: Literal[
        "OWNER_SUPPLIES_VENDOR_SENDER_IDENTITY_AND_CONFIRMS_SUBMISSION_"
        "THEN_ENGINEERING_RESOLVES_BLS_EXACT_BYTE_ACCESS"
    ]
    terminal: Literal[
        "S13_EVIDENCE_RECEIPTS_READY_VENDOR_SEND_AND_BLS_EXACT_BYTES_BLOCKED"
    ]


class HistoricalPITSourceEvidenceAcquisitionReceipt(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_"
        "historical_pit_source_evidence_acquisition_receipt.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_source_evidence_acquisition_receipt_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["AUTHORIZED_EVIDENCE_ACQUISITION_EXECUTED_PARTIAL"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    executed_as_of: date
    predecessor_binding: PredecessorBinding
    owner_approvals: tuple[OwnerApproval, ...]
    vendor_inquiry_packet: VendorInquiryPacket
    official_document_receipts: tuple[OfficialDocumentReceipt, ...]
    failed_official_document_attempts: tuple[FailedOfficialDocumentAttempt, ...]
    revision_gap_assessment: tuple[RevisionGapAssessment, ...]
    source_state: SourceState
    observed_external_activity: ObservedExternalActivity
    temporary_workspace: TemporaryWorkspace
    safety: Safety
    aggregate: Aggregate

    @model_validator(mode="after")
    def validate_surfaces(self) -> Self:
        approvals = tuple(
            (row.request_id, row.risk_tier, row.approval_scope)
            for row in self.owner_approvals
        )
        if approvals != _APPROVAL_SURFACE or tuple(
            row.ordinal for row in self.owner_approvals
        ) != (1, 2):
            raise ValueError("owner approval surface drifted")
        vendors = tuple(
            (row.vendor, row.candidate_id, row.send_state)
            for row in self.vendor_inquiry_packet.vendor_rows
        )
        if vendors != _VENDOR_SURFACE:
            raise ValueError("vendor inquiry surface drifted")
        if tuple(row.document_id for row in self.official_document_receipts) != _RECEIPT_IDS:
            raise ValueError("official document receipt inventory drifted")
        if tuple(row.ordinal for row in self.official_document_receipts) != tuple(range(1, 19)):
            raise ValueError("official document receipt order drifted")
        if tuple(row.document_id for row in self.failed_official_document_attempts) != _FAILED_IDS:
            raise ValueError("failed official document attempt inventory drifted")
        if tuple(row.ordinal for row in self.failed_official_document_attempts) != tuple(
            range(1, 16)
        ):
            raise ValueError("failed official document attempt order drifted")
        if tuple(row.authority for row in self.revision_gap_assessment) != (
            "FED",
            "BLS",
            "BEA",
        ):
            raise ValueError("revision gap authority order drifted")
        if tuple(row.exact_document_receipt_count for row in self.revision_gap_assessment) != (
            6,
            0,
            12,
        ):
            raise ValueError("revision gap receipt counts drifted")
        if sum(row.authority == "FED" for row in self.official_document_receipts) != 6:
            raise ValueError("Fed receipt count drifted")
        if sum(row.authority == "BEA" for row in self.official_document_receipts) != 12:
            raise ValueError("BEA receipt count drifted")
        if sum(row.http_status == 403 for row in self.failed_official_document_attempts) != 10:
            raise ValueError("BLS HTTP 403 attempt count drifted")
        if sum(row.http_status == 404 for row in self.failed_official_document_attempts) != 5:
            raise ValueError("BEA stale-locator HTTP 404 attempt count drifted")
        return self


@dataclass(frozen=True)
class HistoricalPITSourceEvidenceAcquisitionReceiptLoadResult:
    policy: HistoricalPITSourceEvidenceAcquisitionReceipt
    path: Path
    file_sha256: str
    canonical_sha256: str
    s12_evidence_review: s12.HistoricalPITSourceCandidateEvidenceReviewLoadResult
    terminal: Literal[
        "S13_EVIDENCE_RECEIPTS_READY_VENDOR_SEND_AND_BLS_EXACT_BYTES_BLOCKED"
    ]


def load_historical_pit_source_evidence_acquisition_receipt(
    *,
    path: Path = DEFAULT_HISTORICAL_PIT_SOURCE_EVIDENCE_ACQUISITION_RECEIPT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> HistoricalPITSourceEvidenceAcquisitionReceiptLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="s13_acquisition_receipt")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = HistoricalPITSourceEvidenceAcquisitionReceipt.model_validate(payload)
        loaded_s12 = s12.load_historical_pit_source_candidate_evidence_review(
            project_root=project_root
        )
        binding = policy.predecessor_binding
        bound_s12 = _bound_file(Path(binding.path), root=project_root, field="s12_predecessor")
        if hashlib.sha256(bound_s12.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S12 predecessor file SHA-256 mismatch")
        if (binding.file_sha256, binding.canonical_sha256) != (
            loaded_s12.file_sha256,
            loaded_s12.canonical_sha256,
        ):
            raise ValueError("S12 predecessor exact identity drifted")
        predecessor_requests = tuple(
            (row.request_id, row.risk_tier) for row in loaded_s12.policy.owner_decision_requests
        )
        approved_requests = tuple(
            (row.request_id, row.risk_tier) for row in policy.owner_approvals
        )
        if approved_requests != predecessor_requests:
            raise ValueError("S13 approvals do not replay the exact S12 request surface")

        packet = _bound_file(
            Path(policy.vendor_inquiry_packet.path), root=project_root, field="vendor_packet"
        )
        packet_bytes = packet.read_bytes()
        if len(packet_bytes) != policy.vendor_inquiry_packet.byte_count:
            raise ValueError("vendor packet byte count mismatch")
        if hashlib.sha256(packet_bytes).hexdigest() != policy.vendor_inquiry_packet.file_sha256:
            raise ValueError("vendor packet SHA-256 mismatch")

        for receipt in policy.official_document_receipts:
            retained = _bound_file(
                Path(receipt.retained_path),
                root=project_root,
                field=f"official_document:{receipt.document_id}",
            )
            retained_bytes = retained.read_bytes()
            if len(retained_bytes) != receipt.byte_count:
                raise ValueError(f"official document byte count mismatch: {receipt.document_id}")
            if hashlib.sha256(retained_bytes).hexdigest() != receipt.sha256:
                raise ValueError(f"official document SHA-256 mismatch: {receipt.document_id}")
    except HistoricalPITSourceEvidenceAcquisitionReceiptError:
        raise
    except s12.HistoricalPITSourceCandidateEvidenceReviewError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise HistoricalPITSourceEvidenceAcquisitionReceiptError(
            "HISTORICAL_PIT_SOURCE_EVIDENCE_ACQUISITION_RECEIPT_REJECTED", str(exc)
        ) from exc
    return HistoricalPITSourceEvidenceAcquisitionReceiptLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        s12_evidence_review=loaded_s12,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_HISTORICAL_PIT_SOURCE_EVIDENCE_ACQUISITION_RECEIPT_PATH",
    "HistoricalPITSourceEvidenceAcquisitionReceipt",
    "HistoricalPITSourceEvidenceAcquisitionReceiptError",
    "HistoricalPITSourceEvidenceAcquisitionReceiptLoadResult",
    "load_historical_pit_source_evidence_acquisition_receipt",
]
