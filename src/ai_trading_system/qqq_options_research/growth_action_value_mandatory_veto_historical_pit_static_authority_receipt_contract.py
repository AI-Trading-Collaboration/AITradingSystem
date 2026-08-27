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
    growth_action_value_mandatory_veto_historical_pit_receipt_authority_decision_pack as s10,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_HISTORICAL_PIT_STATIC_AUTHORITY_RECEIPT_CONTRACT_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "historical_pit_static_authority_receipt_contract_v1.yaml"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_ID = re.compile(r"^[0-9a-f]{40}$")
_TERMINAL: Literal[
    "S11_STATIC_AUTHORITY_RECEIPT_CONTRACT_FROZEN_HISTORICAL_COVERAGE_UNPROVEN"
] = "S11_STATIC_AUTHORITY_RECEIPT_CONTRACT_FROZEN_HISTORICAL_COVERAGE_UNPROVEN"
_HISTORICAL_CLASSES = (
    "PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE",
    "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
    "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
)
_AUTHORITY_ROLES = (
    "HISTORICAL_PRICE_PUBLICATION_AUTHORITY",
    "HISTORICAL_VOLATILITY_PUBLICATION_AUTHORITY",
    "INITIAL_SCHEDULE_AUTHORITY",
    "REVISION_NOTICE_AUTHORITY",
    "IMMUTABLE_CAPTURE_AUTHORITY",
    "TERMINAL_RECONCILIATION_ONLY",
    "RESULT_RELEASE_NOT_SCHEDULE_AUTHORITY",
)
_PRIMARY_WINDOW = (
    "XNYS",
    date(2021, 2, 22),
    date(2025, 12, 2),
    1202,
    "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0",
)
_RECEIPT_FIELDS = (
    "authority_id",
    "authority_class",
    "authority_role",
    "source_document_id",
    "source_url",
    "payload_sha256",
    "source_native_version_id",
    "published_at",
    "available_at",
    "available_at_precision",
    "trusted_captured_at",
    "capture_authority_id",
    "receipt_id",
    "supersedes_receipt_id",
    "downloaded_at_audit_only",
)
_PROHIBITED_AVAILABLE_AT_INFERENCE = (
    "DOWNLOADED_AT_AS_AVAILABLE_AT",
    "CURRENT_PAGE_LAST_MODIFIED_AS_HISTORICAL_AVAILABLE_AT",
    "SCHEDULED_FOR_AS_AVAILABLE_AT",
    "EVENT_OR_RESULT_DATE_AS_AVAILABLE_AT",
    "SESSION_PLUS_ONE_AS_SOURCE_PROVEN_AVAILABLE_AT",
)
_SCHEDULE_FIELDS = (
    "authority_id",
    "authority_role",
    "source_document_id",
    "source_url",
    "payload_sha256",
    "source_year",
    "stable_event_key",
    "event_taxonomy_id",
    "reference_period_or_meeting_ordinal",
    "scheduled_for",
    "revision_action",
    "source_native_revision_id",
    "revision_id",
    "published_at",
    "available_at",
    "available_at_precision",
    "trusted_captured_at",
    "capture_authority_id",
    "supersedes_revision_id",
    "supersession_reason",
    "coverage_through",
    "receipt_id",
    "downloaded_at_audit_only",
)
_REVISION_ACTIONS = (
    "ADD",
    "MOVE",
    "TIME_CHANGE",
    "CANCEL",
    "RESTORE",
    "METADATA_CORRECTION",
)
_INVENTORY_CATEGORIES = (
    "REGULAR_SCHEDULED",
    "SPECIAL_OR_EMERGENCY",
    "NOTATION_VOTE",
    "RESCHEDULE_NOTICE",
    "CANCELLATION",
    "RESTORATION",
)
_FROZEN_EVENT_TYPE_IDS = (
    "FEDERAL_RESERVE:FOMC_RATE_DECISION",
    "BLS:CPI",
    "BLS:NONFARM_PAYROLLS",
    "BEA:PCE_PRICE_INDEX",
    "BEA:GDP_ADVANCE_ESTIMATE",
)
_ORDERED_STATES = (
    "CANDIDATE_SOURCE_APPROVED",
    "EXACT_AUTHORITY_IDENTITY_FROZEN",
    "SOURCE_CONTRACT_ADMITTED",
    "HISTORICAL_COVERAGE_PROVEN",
    "STATIC_MANIFEST_REPLAY_PASS",
    "REAL_SOURCE_QUERY_AUTHORIZED",
    "OBSERVED_INVENTORY_ADMITTED",
    "REAL_DQ_AUTHORIZED",
    "REAL_DQ_PASS",
    "VETO_SERIES_GENERATION_AUTHORIZED",
    "BACKTEST_AUTHORIZED",
    "BACKTEST_VALIDITY_REVIEWED",
    "PAPER_LIVE_PRODUCTION_BROKER_AUTHORIZED",
)
_SOURCE_SURFACE = (
    (
        "fmp_spy_qqq_eod_adjusted_v1",
        "FMP_HISTORICAL_ROW_AVAILABLE_AT_UNPROVEN",
        "Financial Modeling Prep",
        "VENDOR_EVIDENCE_REQUIRED",
        "VENDOR_CERTIFIED_VERSIONED_AS_OF_SPY_QQQ_EOD_ARCHIVE",
        False,
        False,
        ("HISTORICAL_PRICE_PUBLICATION_AUTHORITY",),
        "PUBLIC_PREMIUM_APPROX_USD_59_MONTH_ANNUAL_BILLING_ENTERPRISE_QUOTE_REQUIRED",
        "PREPARE_NOT_SENT",
    ),
    (
        "cboe_vix_index_daily_v1",
        "CBOE_VIX_HISTORICAL_PUBLICATION_VINTAGE_UNPROVEN",
        "Cboe Global Markets",
        "VENDOR_EVIDENCE_REQUIRED",
        "CBOE_DATASHOP_MAIN_CHANNEL_END_OF_DAY_VIX_DAILY_FILES",
        False,
        False,
        ("HISTORICAL_VOLATILITY_PUBLICATION_AUTHORITY",),
        "PUBLIC_FIXED_PRICE_UNVERIFIED_QUOTE_REQUIRED",
        "PREPARE_NOT_SENT",
    ),
    (
        "federal_reserve_fomc_schedule_capture_v1",
        "FED_FOMC_REVISION_LEDGER_UNAVAILABLE",
        "Federal Reserve Board",
        "FREEZE_CANDIDATE",
        "FEDERAL_RESERVE_ANNUAL_TENTATIVE_FOMC_SCHEDULE_PRESS_RELEASE_SERIES_2021_2025",
        True,
        True,
        ("INITIAL_SCHEDULE_AUTHORITY", "TERMINAL_RECONCILIATION_ONLY"),
        "PUBLIC_OFFICIAL_NO_LICENSE_FEE_IDENTIFIED",
        "EXACT_URL_DIGEST_REVISION_INVENTORY_NOT_GENERATED",
    ),
    (
        "bls_release_schedule_capture_v1",
        "BLS_RELEASE_SCHEDULE_REVISION_LEDGER_UNAVAILABLE",
        "Bureau of Labor Statistics",
        "INVENTORY_ONLY",
        "BLS_ANNUAL_RELEASE_SCHEDULE_AND_OFFICIAL_RESCHEDULE_NOTICE_ARCHIVE",
        False,
        False,
        (
            "INITIAL_SCHEDULE_AUTHORITY",
            "REVISION_NOTICE_AUTHORITY",
            "TERMINAL_RECONCILIATION_ONLY",
        ),
        "PUBLIC_OFFICIAL_NO_LICENSE_FEE_IDENTIFIED",
        "ARCHIVE_INVENTORY_NOT_GENERATED",
    ),
    (
        "bea_release_schedule_capture_v1",
        "BEA_FROZEN_ENDPOINT_NOT_SCHEDULE_REVISION_AUTHORITY",
        "Bureau of Economic Analysis",
        "INVENTORY_ONLY",
        "BEA_SCB_ANNUAL_SCHEDULE_UPDATE_NOTICE_AND_ARCHIVED_RELEASE_SERIES",
        False,
        False,
        (
            "INITIAL_SCHEDULE_AUTHORITY",
            "REVISION_NOTICE_AUTHORITY",
            "TERMINAL_RECONCILIATION_ONLY",
        ),
        "PUBLIC_OFFICIAL_NO_LICENSE_FEE_IDENTIFIED",
        "ARCHIVE_INVENTORY_NOT_GENERATED_FIVE_PROMPT_PDF_URLS_RETURNED_404",
    ),
)
_FALSIFICATION_SURFACE = (
    (
        "AUTHORITY_IDENTITY_AND_CLASS",
        "LOCK_THIS_AXIS_ONLY",
        "REJECT_CANDIDATE_SEARCH_ALTERNATE",
        "KEEP_UNFROZEN_AND_BLOCKED",
        "QUARANTINE_EVIDENCE_AND_STOP_LANE",
    ),
    (
        "IMMUTABLE_BYTES_AND_DIGEST",
        "BIND_EXACT_DIGEST_AND_RECEIPT",
        "REJECT_CANDIDATE_UNSTABLE_OR_UNRETAINED",
        "KEEP_BLOCKED_MISSING_DIGEST_OR_INVENTORY",
        "QUARANTINE_DIGEST_MISMATCH_OR_CONTENT_DRIFT",
    ),
    (
        "HISTORICAL_AVAILABLE_AT",
        "ADMIT_RECEIPT_TO_CUTOFF_REPLAY_ONLY",
        "REJECT_CANDIDATE_NO_HISTORICAL_PUBLICATION_VINTAGE",
        "KEEP_BLOCKED_TIMESTAMP_UNPROVEN",
        "REJECT_INFERRED_OR_CURRENT_TIMESTAMP",
    ),
    (
        "ADJUSTMENT_OR_REVISION_LINEAGE",
        "BIND_COMPLETE_APPEND_ONLY_LINEAGE",
        "REJECT_CANDIDATE_IRRECOVERABLE_LINEAGE_GAP",
        "KEEP_BLOCKED_REISSUE_OR_SUPERSESSION_UNKNOWN",
        "QUARANTINE_LATER_REVISION_OVERWRITE_OR_DELETION",
    ),
    (
        "EVENT_UNIVERSE_AND_TAXONOMY",
        "MAP_EXACT_FROZEN_EVENT_TYPE_IDS_ONLY",
        "REJECT_AUTHORITY_MISSING_FROZEN_EVENT_TYPE",
        "REQUIRE_OWNER_TAXONOMY_DECISION",
        "REJECT_LANE_LOCAL_EVENT_UNIVERSE_DRIFT",
    ),
    (
        "EXACT_WINDOW_COVERAGE",
        "BIND_EXACT_1202_WARMUP_STATE_AND_CUTOFF_PROOF",
        "REJECT_CANDIDATE_PROVEN_COVERAGE_GAP",
        "KEEP_BLOCKED_INVENTORY_INCOMPLETE",
        "REJECT_FINAL_ROW_COUNT_OR_LATER_REVISION_AS_COVERAGE",
    ),
    (
        "LICENSE_AND_INTERNAL_USE",
        "BIND_EXACT_PERMITTED_USE",
        "REJECT_CANDIDATE_PROHIBITED_USE",
        "KEEP_BLOCKED_TERMS_OR_PRICE_UNCONFIRMED",
        "REJECT_GUESSED_LICENSE_SCOPE",
    ),
    (
        "ADAPTER_AND_MANIFEST_COMPATIBILITY",
        "BIND_EXISTING_CONTRACT_FIELDS_WITHOUT_DRIFT",
        "REJECT_CANDIDATE_MISSING_REQUIRED_FIELDS",
        "KEEP_BLOCKED_MAPPING_INCOMPLETE",
        "REJECT_SILENT_FIELD_DELETION_OR_SEMANTIC_CHANGE",
    ),
    (
        "STATE_SEPARATION",
        "ADVANCE_ONLY_THE_PROVEN_STATE",
        "REJECT_NON_SEPARABLE_FLOW",
        "KEEP_BLOCKED_STATE_UNDEFINED",
        "REJECT_CAPABILITY_TO_RUNTIME_OR_INVESTMENT_ESCALATION",
    ),
    (
        "SAFETY_COUNTERS",
        "KEEP_ALL_COUNTERS_ZERO_UNTIL_SEPARATE_GATE",
        "STOP_ON_UNAUTHORIZED_ACTION",
        "KEEP_BLOCKED_AUDIT_COUNTERS_INCOMPLETE",
        "QUARANTINE_HIDDEN_RESET_OR_UNATTRIBUTED_COUNTER",
    ),
)


class HistoricalPITStaticAuthorityReceiptContractError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
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
        "owner_decision:TRADING-2542G:S11:2026-08-27:"
        "adopt_web_pro_static_authority_receipt_contract_sequence_v1"
    ]
    scope: Literal[
        "NON_EXECUTABLE_DATA_RESEARCH_STATIC_AUTHORITY_RECEIPT_CONTRACT_ONLY"
    ]
    candidate_disposition_freeze_allowed: Literal[True]
    candidate_authority_family_freeze_allowed: Literal[True]
    exact_authority_selection_allowed: Literal[False]
    vendor_contact_allowed: Literal[False]
    paid_product_purchase_allowed: Literal[False]
    provider_query_allowed: Literal[False]


class PredecessorBinding(_StrictModel):
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_receipt_authority_decision_pack_v1"
    ]
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_receipt_authority_decision_pack_v1.yaml"
    ]
    file_sha256: str
    canonical_sha256: str
    role: Literal["S10_HISTORICAL_PIT_AUTHORITY_DECISION_SURFACE"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class CompatibilityCloseout(_StrictModel):
    candidate_sha: str
    failed_parent_path: Literal[
        "outputs/validation_runtime/full_20260827T025319Z/test_runtime_summary.json"
    ]
    failed_parent_sha256: str
    passing_full_path: Literal[
        "outputs/validation_runtime/full_20260827T123031Z/test_runtime_summary.json"
    ]
    passing_full_sha256: str
    passing_full_result: Literal["9787_PASSED_3_SKIPPED"]
    changes_source_blockers: Literal[False]

    @field_validator("candidate_sha")
    @classmethod
    def validate_candidate_sha(cls, value: str) -> str:
        if not _GIT_OBJECT_ID.fullmatch(value):
            raise ValueError("invalid lowercase Git SHA-1")
        return value

    @field_validator("failed_parent_sha256", "passing_full_sha256")
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


AuthorityClass = Literal[
    "PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE",
    "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
    "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
]
AuthorityRole = Literal[
    "HISTORICAL_PRICE_PUBLICATION_AUTHORITY",
    "HISTORICAL_VOLATILITY_PUBLICATION_AUTHORITY",
    "INITIAL_SCHEDULE_AUTHORITY",
    "REVISION_NOTICE_AUTHORITY",
    "IMMUTABLE_CAPTURE_AUTHORITY",
    "TERMINAL_RECONCILIATION_ONLY",
    "RESULT_RELEASE_NOT_SCHEDULE_AUTHORITY",
]
CandidateDisposition = Literal[
    "FREEZE_CANDIDATE", "INVENTORY_ONLY", "VENDOR_EVIDENCE_REQUIRED", "REJECT"
]


class AuthorityClassPolicy(_StrictModel):
    historical_candidate_classes: tuple[AuthorityClass, ...]
    forward_only_class: Literal["FORWARD_ONLY_CAPTURE_LEDGER"]
    rejected_class: Literal["INFERRED_OR_CURRENT_STATE_SUBSTITUTE"]
    forward_only_counts_as_historical_coverage: Literal[False]
    inferred_timestamp_counts_as_pit_evidence: Literal[False]
    current_endpoint_state_counts_as_historical_vintage: Literal[False]


class AuthorityRolePolicy(_StrictModel):
    allowed_roles: tuple[AuthorityRole, ...]
    terminal_reconciliation_counts_as_historical_available_at: Literal[False]
    result_release_counts_as_schedule_authority: Literal[False]


class ReceiptIdentityContract(_StrictModel):
    required_fields: tuple[str, ...]
    downloaded_at_role: Literal["AUDIT_ONLY_NOT_PIT"]
    date_only_intraday_cutoff_policy: Literal[
        "INSUFFICIENT_UNLESS_OWNER_FROZEN_CONSERVATIVE_MAPPING"
    ]
    prohibited_available_at_inference: tuple[str, ...]


class ScheduleRevisionLedgerContract(_StrictModel):
    required_fields: tuple[str, ...]
    revision_actions: tuple[
        Literal["ADD", "MOVE", "TIME_CHANGE", "CANCEL", "RESTORE", "METADATA_CORRECTION"],
        ...,
    ]
    append_only: Literal[True]
    stable_event_key_may_use_scheduled_for_only: Literal[False]
    superseded_revisions_retained: Literal[True]
    move_then_restore_records_two_revisions: Literal[True]
    current_or_final_calendar_role: Literal["TERMINAL_RECONCILIATION_ONLY"]
    final_calendar_can_supply_historical_available_at: Literal[False]


class EventTaxonomyContract(_StrictModel):
    inventory_categories: tuple[str, ...]
    frozen_veto_event_type_ids: tuple[str, ...]
    only_exact_frozen_event_type_ids_affect_veto: Literal[True]
    special_or_emergency_default: Literal["INVENTORY_ONLY_NOT_VETO_ELIGIBLE"]
    notation_vote_default: Literal["INVENTORY_ONLY_NOT_VETO_ELIGIBLE"]
    unmapped_event_terminal: Literal["INSUFFICIENT_OWNER_EVENT_TAXONOMY_REQUIRED"]
    architecture_expansion_allowed: Literal[False]


class CutoffCoverageContract(_StrictModel):
    decision_cutoff_scope: Literal["EXACT_1202_ORDERED_DECISION_CUTOFFS"]
    selected_revision_rule: Literal[
        "LATEST_ADMITTED_REVISION_WITH_AVAILABLE_AT_LTE_CUTOFF"
    ]
    required_coverage_through: Literal["NEXT_ACTION_SESSION_CLOSE"]
    later_revision_leakage_allowed: Literal[False]
    unresolved_conflict_allowed: Literal[False]
    complete_receipt_lineage_required: Literal[True]
    exact_warmup_and_state_lineage_required: Literal[True]
    target_row_count_alone_proves_coverage: Literal[False]


class StateSeparationContract(_StrictModel):
    ordered_states: tuple[str, ...]
    automatic_state_promotion_allowed: Literal[False]
    partial_source_freeze_can_generate_series: Literal[False]
    source_capability_implies_investment_value: Literal[False]


class SourceDispositionRow(_StrictModel):
    ordinal: int
    candidate_id: str
    blocker_reason_code: str
    provider_or_authority: str
    candidate_disposition: CandidateDisposition
    disposition_frozen: Literal[True]
    candidate_authority_family: str
    candidate_authority_family_frozen: bool
    candidate_source_approved: bool
    acceptable_historical_authority_classes: tuple[AuthorityClass, ...]
    authority_roles: tuple[AuthorityRole, ...]
    reference_locators: tuple[str, ...]
    rejected_authority_locators: tuple[str, ...]
    cost_state: str
    evidence_packet_state: str
    required_next_evidence: tuple[str, ...]
    selected_authority_class: Literal[None]
    exact_authority_identity: Literal[None]
    exact_authority_identity_frozen: Literal[False]
    historical_coverage_proven: Literal[False]
    source_contract_admitted: Literal[False]
    runtime_authorized: Literal[False]
    blocker_remediated: Literal[False]

    @model_validator(mode="after")
    def validate_source_row(self) -> Self:
        for label, values in (
            ("acceptable authority classes", self.acceptable_historical_authority_classes),
            ("authority roles", self.authority_roles),
            ("reference locators", self.reference_locators),
            ("required next evidence", self.required_next_evidence),
        ):
            if not values or len(set(values)) != len(values):
                raise ValueError(f"{label} must be non-empty and unique")
        if len(set(self.rejected_authority_locators)) != len(
            self.rejected_authority_locators
        ):
            raise ValueError("rejected authority locators must be unique")
        return self


class FalsificationAxis(_StrictModel):
    axis_id: str
    pass_action: str
    fail_action: str
    insufficient_action: str
    invalid_action: str


class Aggregate(_StrictModel):
    source_row_count: Literal[5]
    disposition_frozen_count: Literal[5]
    candidate_source_approved_count: Literal[1]
    exact_authority_identity_frozen_count: Literal[0]
    historical_coverage_proven_count: Literal[0]
    source_contract_admitted_count: Literal[0]
    runtime_authorized_count: Literal[0]
    remediated_blocker_count: Literal[0]
    s10_technical_validation_state: Literal["BLOCKED"]
    s9_manifest_replay_state: Literal["BLOCKED"]
    next_legal_action: Literal[
        "BUILD_READ_ONLY_OFFICIAL_SCHEDULE_INVENTORY_AND_VENDOR_EVIDENCE_PACKETS_NOT_SENT"
    ]
    terminal: Literal[
        "S11_STATIC_AUTHORITY_RECEIPT_CONTRACT_FROZEN_HISTORICAL_COVERAGE_UNPROVEN"
    ]


class ActualCounters(_StrictModel):
    network_request_count: Literal[0]
    provider_query_attempt_count: Literal[0]
    vendor_contact_count: Literal[0]
    paid_product_purchase_count: Literal[0]
    cache_read_count: Literal[0]
    market_data_file_read_count: Literal[0]
    real_market_payload_download_count: Literal[0]
    real_payload_adapter_execution_count: Literal[0]
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
    static_tracked_authority_reads_allowed: Literal[True]
    provider_query_allowed: Literal[False]
    vendor_contact_allowed: Literal[False]
    paid_product_purchase_allowed: Literal[False]
    network_io_allowed: Literal[False]
    cache_read_allowed: Literal[False]
    market_data_file_read_allowed: Literal[False]
    real_market_payload_download_allowed: Literal[False]
    real_payload_adapter_execution_allowed: Literal[False]
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


class HistoricalPITStaticAuthorityReceiptContract(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_"
        "historical_pit_static_authority_receipt_contract.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_static_authority_receipt_contract_v1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_APPROVED_STATIC_CONTRACT_HISTORICAL_COVERAGE_UNPROVEN"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    owner_scope: OwnerScope
    predecessor_binding: PredecessorBinding
    compatibility_closeout: CompatibilityCloseout
    primary_research_window: PrimaryResearchWindow
    authority_class_policy: AuthorityClassPolicy
    authority_role_policy: AuthorityRolePolicy
    receipt_identity_contract: ReceiptIdentityContract
    schedule_revision_ledger_contract: ScheduleRevisionLedgerContract
    event_taxonomy_contract: EventTaxonomyContract
    cutoff_coverage_contract: CutoffCoverageContract
    state_separation_contract: StateSeparationContract
    source_rows: tuple[SourceDispositionRow, ...]
    falsification_axes: tuple[FalsificationAxis, ...]
    aggregate: Aggregate
    actual_counters: ActualCounters
    safety: Safety

    @model_validator(mode="after")
    def validate_static_contract_surface(self) -> Self:
        if self.authority_class_policy.historical_candidate_classes != _HISTORICAL_CLASSES:
            raise ValueError("historical authority class policy drifted")
        if self.authority_role_policy.allowed_roles != _AUTHORITY_ROLES:
            raise ValueError("authority role policy drifted")
        if self.receipt_identity_contract.required_fields != _RECEIPT_FIELDS:
            raise ValueError("receipt identity contract drifted")
        if (
            self.receipt_identity_contract.prohibited_available_at_inference
            != _PROHIBITED_AVAILABLE_AT_INFERENCE
        ):
            raise ValueError("available-at inference policy drifted")
        if self.schedule_revision_ledger_contract.required_fields != _SCHEDULE_FIELDS:
            raise ValueError("schedule revision receipt contract drifted")
        if self.schedule_revision_ledger_contract.revision_actions != _REVISION_ACTIONS:
            raise ValueError("schedule revision action policy drifted")
        if self.event_taxonomy_contract.inventory_categories != _INVENTORY_CATEGORIES:
            raise ValueError("event inventory taxonomy drifted")
        if self.event_taxonomy_contract.frozen_veto_event_type_ids != _FROZEN_EVENT_TYPE_IDS:
            raise ValueError("frozen veto event taxonomy drifted")
        if self.state_separation_contract.ordered_states != _ORDERED_STATES:
            raise ValueError("state separation contract drifted")
        observed_window = (
            self.primary_research_window.exchange_calendar,
            self.primary_research_window.requested_start,
            self.primary_research_window.requested_end,
            self.primary_research_window.exact_session_count,
            self.primary_research_window.ordered_session_ids_lf_sha256,
        )
        if observed_window != _PRIMARY_WINDOW:
            raise ValueError("primary research window policy drifted")
        observed_sources = tuple(
            (
                row.candidate_id,
                row.blocker_reason_code,
                row.provider_or_authority,
                row.candidate_disposition,
                row.candidate_authority_family,
                row.candidate_authority_family_frozen,
                row.candidate_source_approved,
                row.authority_roles,
                row.cost_state,
                row.evidence_packet_state,
            )
            for row in self.source_rows
        )
        if observed_sources != _SOURCE_SURFACE:
            raise ValueError("five-source candidate disposition surface drifted")
        if tuple(row.ordinal for row in self.source_rows) != tuple(range(1, 6)):
            raise ValueError("five-source candidate order drifted")
        observed_falsification = tuple(
            (
                row.axis_id,
                row.pass_action,
                row.fail_action,
                row.insufficient_action,
                row.invalid_action,
            )
            for row in self.falsification_axes
        )
        if observed_falsification != _FALSIFICATION_SURFACE:
            raise ValueError("falsification and stop matrix drifted")
        approved = sum(row.candidate_source_approved for row in self.source_rows)
        if approved != self.aggregate.candidate_source_approved_count:
            raise ValueError("candidate source approved aggregate drifted")
        return self


@dataclass(frozen=True)
class HistoricalPITStaticAuthorityReceiptContractLoadResult:
    policy: HistoricalPITStaticAuthorityReceiptContract
    path: Path
    file_sha256: str
    canonical_sha256: str
    s10_decision_pack: s10.HistoricalPITReceiptAuthorityDecisionPackLoadResult
    terminal: Literal[
        "S11_STATIC_AUTHORITY_RECEIPT_CONTRACT_FROZEN_HISTORICAL_COVERAGE_UNPROVEN"
    ]


def load_historical_pit_static_authority_receipt_contract(
    *,
    path: Path = DEFAULT_HISTORICAL_PIT_STATIC_AUTHORITY_RECEIPT_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> HistoricalPITStaticAuthorityReceiptContractLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="s11_static_contract")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)
        loaded_s10 = s10.load_historical_pit_receipt_authority_decision_pack(
            project_root=project_root
        )
        binding = policy.predecessor_binding
        bound_s10 = _bound_file(Path(binding.path), root=project_root, field="s10_predecessor")
        if hashlib.sha256(bound_s10.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S10 predecessor file SHA-256 mismatch")
        if (binding.file_sha256, binding.canonical_sha256) != (
            loaded_s10.file_sha256,
            loaded_s10.canonical_sha256,
        ):
            raise ValueError("S10 predecessor exact identity drifted")

        s10_rows = loaded_s10.policy.decision_rows
        predecessor_surface = tuple(
            (
                row.candidate_id,
                row.blocker_reason_code,
                row.acceptable_historical_authority_classes,
            )
            for row in s10_rows
        )
        s11_surface = tuple(
            (
                row.candidate_id,
                row.blocker_reason_code,
                row.acceptable_historical_authority_classes,
            )
            for row in policy.source_rows
        )
        if s11_surface != predecessor_surface:
            raise ValueError("S11 source rows do not replay the exact S10 blocker surface")

        predecessor_window = loaded_s10.policy.primary_research_window
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
            raise ValueError("S11 primary research window drifted from S10")
    except HistoricalPITStaticAuthorityReceiptContractError:
        raise
    except s10.HistoricalPITReceiptAuthorityDecisionPackError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise HistoricalPITStaticAuthorityReceiptContractError(
            "HISTORICAL_PIT_STATIC_AUTHORITY_RECEIPT_CONTRACT_REJECTED", str(exc)
        ) from exc
    return HistoricalPITStaticAuthorityReceiptContractLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        s10_decision_pack=loaded_s10,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_HISTORICAL_PIT_STATIC_AUTHORITY_RECEIPT_CONTRACT_PATH",
    "HistoricalPITStaticAuthorityReceiptContract",
    "HistoricalPITStaticAuthorityReceiptContractError",
    "HistoricalPITStaticAuthorityReceiptContractLoadResult",
    "load_historical_pit_static_authority_receipt_contract",
]
