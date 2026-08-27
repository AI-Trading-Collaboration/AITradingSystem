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
    growth_action_value_mandatory_veto_manifest_replay_gate as s9,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_HISTORICAL_PIT_RECEIPT_AUTHORITY_DECISION_PACK_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "historical_pit_receipt_authority_decision_pack_v1.yaml"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL: Literal[
    "OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED"
] = "OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED"
_FORWARD_ONLY_IMPACT = (
    "DOES_NOT_UNBLOCK_PRIMARY_2021_02_22_WINDOW_"
    "ONLY_SUPPORTS_POST_CAPTURE_FORWARD_RESEARCH"
)
_HISTORICAL_CLASSES = (
    "PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE",
    "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
    "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
)
_PRIMARY_WINDOW = (
    "XNYS",
    date(2021, 2, 22),
    date(2025, 12, 2),
    1202,
    "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0",
)
_MINIMUM_RECEIPT_FIELDS = {
    "fmp_spy_qqq_eod_adjusted_v1": (
        "source_identity",
        "endpoint_identity",
        "request_parameters",
        "ticker",
        "source_session",
        "row_available_at",
        "downloaded_at",
        "raw_close",
        "adjusted_close",
        "adjustment_basis",
        "corporate_action_vintage",
        "schema_version",
        "row_count",
        "payload_sha256",
        "supersession_id",
    ),
    "cboe_vix_index_daily_v1": (
        "official_source_identity",
        "endpoint_identity",
        "source_session",
        "vix_level_definition",
        "publication_or_trusted_capture_at",
        "available_at",
        "observation_session_mapping",
        "revision_policy",
        "schema_version",
        "row_count",
        "payload_sha256",
        "supersession_id",
    ),
    "federal_reserve_fomc_schedule_capture_v1": (
        "official_authority_identity",
        "endpoint_identity",
        "stable_event_key",
        "event_taxonomy",
        "scheduled_for",
        "revision_id",
        "revision_action",
        "published_at_or_trusted_captured_at",
        "available_at",
        "coverage_through",
        "payload_sha256",
        "supersedes_revision_id",
    ),
    "bls_release_schedule_capture_v1": (
        "official_authority_identity",
        "endpoint_identity",
        "stable_event_key",
        "event_taxonomy",
        "scheduled_for",
        "revision_id",
        "revision_action",
        "published_at_or_trusted_captured_at",
        "available_at",
        "coverage_through",
        "payload_sha256",
        "supersedes_revision_id",
    ),
    "bea_release_schedule_capture_v1": (
        "official_schedule_authority_identity",
        "schedule_endpoint_identity",
        "stable_event_key",
        "event_taxonomy",
        "scheduled_for",
        "revision_id",
        "revision_action",
        "published_at_or_trusted_captured_at",
        "available_at",
        "coverage_through",
        "payload_sha256",
        "supersedes_revision_id",
    ),
}
_DECISION_SURFACE = (
    (
        "fmp_spy_qqq_eod_adjusted_v1",
        "FMP_HISTORICAL_ROW_AVAILABLE_AT_UNPROVEN",
        "EOD_PRICE_AND_ADJUSTMENT_VINTAGE",
        (
            "PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE",
            "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
        ),
        "TARGET_1202_PLUS_EXACT_WARMUP_AND_ADJUSTMENT_VINTAGE_LINEAGE",
        "PROVIDE_OR_APPROVE_EXACT_VERSIONED_PRICE_ARCHIVE_IDENTITY",
        False,
    ),
    (
        "cboe_vix_index_daily_v1",
        "CBOE_VIX_HISTORICAL_PUBLICATION_VINTAGE_UNPROVEN",
        "IMPLIED_VOLATILITY_PUBLICATION_VINTAGE",
        (
            "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
            "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
        ),
        "TARGET_1202_PLUS_251_PRETARGET_OBSERVATIONS_WITH_PUBLICATION_VINTAGE",
        "PROVIDE_OR_APPROVE_EXACT_VERSIONED_VIX_ARCHIVE_IDENTITY",
        False,
    ),
    (
        "federal_reserve_fomc_schedule_capture_v1",
        "FED_FOMC_REVISION_LEDGER_UNAVAILABLE",
        "OFFICIAL_SCHEDULE_REVISION_LEDGER",
        (
            "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
            "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
        ),
        "EVERY_DECISION_CUTOFF_THROUGH_NEXT_ACTION_CLOSE_WITH_REVISION_LEDGER",
        "PROVIDE_OR_APPROVE_EXACT_FOMC_SCHEDULE_ARCHIVE_IDENTITY",
        False,
    ),
    (
        "bls_release_schedule_capture_v1",
        "BLS_RELEASE_SCHEDULE_REVISION_LEDGER_UNAVAILABLE",
        "OFFICIAL_SCHEDULE_REVISION_LEDGER",
        (
            "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
            "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
        ),
        "EVERY_DECISION_CUTOFF_THROUGH_NEXT_ACTION_CLOSE_WITH_REVISION_LEDGER",
        "PROVIDE_OR_APPROVE_EXACT_BLS_SCHEDULE_ARCHIVE_IDENTITY",
        False,
    ),
    (
        "bea_release_schedule_capture_v1",
        "BEA_FROZEN_ENDPOINT_NOT_SCHEDULE_REVISION_AUTHORITY",
        "OFFICIAL_SCHEDULE_REVISION_LEDGER",
        (
            "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
            "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
        ),
        "EVERY_DECISION_CUTOFF_THROUGH_NEXT_ACTION_CLOSE_WITH_REVISION_LEDGER",
        "APPROVE_CORRECTED_OFFICIAL_BEA_SCHEDULE_AUTHORITY_AND_VERSIONED_LEDGER",
        True,
    ),
)


class HistoricalPITReceiptAuthorityDecisionPackError(ValueError):
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
        "owner_instruction:TRADING-2542G:S10:2026-08-27:"
        "continue_non_executable_historical_pit_authority_decision_pack_v1"
    ]
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH_DECISION_PACK_DRAFT_ONLY"]
    decision_state: Literal["PROPOSAL_NOT_OWNER_APPROVED"]
    exact_authority_selection_allowed: Literal[False]
    provider_query_allowed: Literal[False]


class PredecessorBinding(_StrictModel):
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_manifest_replay_gate_v1"
    ]
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_manifest_replay_gate_v1.yaml"
    ]
    file_sha256: str
    canonical_sha256: str
    role: Literal["S9_EXACT_MANIFEST_REPLAY_BLOCKER_AUTHORITY"]
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


AuthorityClass = Literal[
    "PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE",
    "OFFICIAL_VERSIONED_SOURCE_ARCHIVE",
    "IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE",
]


class AuthorityClassPolicy(_StrictModel):
    historical_candidate_classes: tuple[AuthorityClass, ...]
    forward_only_class: Literal["FORWARD_ONLY_CAPTURE_LEDGER"]
    rejected_class: Literal["INFERRED_OR_CURRENT_STATE_SUBSTITUTE"]
    forward_only_counts_as_historical_coverage: Literal[False]
    inferred_timestamp_counts_as_pit_evidence: Literal[False]
    current_endpoint_state_counts_as_historical_vintage: Literal[False]


class AuthorityDecisionRow(_StrictModel):
    ordinal: int
    candidate_id: str
    blocker_reason_code: str
    current_endpoint: str
    source_family: str
    acceptable_historical_authority_classes: tuple[AuthorityClass, ...]
    minimum_receipt_fields: tuple[str, ...]
    coverage_contract: str
    forward_only_impact: Literal[
        "DOES_NOT_UNBLOCK_PRIMARY_2021_02_22_WINDOW_"
        "ONLY_SUPPORTS_POST_CAPTURE_FORWARD_RESEARCH"
    ]
    rejected_substitutes: tuple[str, ...]
    recommended_owner_action: str
    endpoint_contract_correction_required: bool
    owner_decision_state: Literal["PROPOSAL_NOT_OWNER_APPROVED"]
    exact_authority_identity: Literal[None]
    historical_coverage_proven: Literal[False]
    source_contract_admitted: Literal[False]
    blocker_remediated: Literal[False]

    @model_validator(mode="after")
    def validate_decision_row(self) -> Self:
        if not self.minimum_receipt_fields or len(set(self.minimum_receipt_fields)) != len(
            self.minimum_receipt_fields
        ):
            raise ValueError("minimum receipt fields must be non-empty and unique")
        if not self.rejected_substitutes or len(set(self.rejected_substitutes)) != len(
            self.rejected_substitutes
        ):
            raise ValueError("rejected substitutes must be non-empty and unique")
        if not self.acceptable_historical_authority_classes:
            raise ValueError("at least one historical authority class is required")
        return self


class AggregateDecision(_StrictModel):
    decision_row_count: Literal[5]
    owner_approved_authority_count: Literal[0]
    historical_coverage_proven_count: Literal[0]
    source_contract_admitted_count: Literal[0]
    remediated_blocker_count: Literal[0]
    s9_authorization_state: Literal["STANDING_OWNER_SCOPE"]
    s9_technical_validation_state: Literal["BLOCKED"]
    primary_window_manifest_replay_state: Literal["BLOCKED"]
    recommendation: Literal[
        "ACQUIRE_OR_PROVIDE_EXACT_HISTORICAL_PIT_AUTHORITY_FOR_ALL_FIVE_"
        "THEN_CREATE_SEPARATE_FREEZE_WAVE"
    ]
    next_legal_action: Literal[
        "OWNER_PROVIDE_OR_EXACT_APPROVE_FIVE_ARCHIVE_IDENTITIES_"
        "THEN_BUILD_STATIC_FREEZE_ADMISSION"
    ]
    terminal: Literal[
        "OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED"
    ]


class ActualCounters(_StrictModel):
    network_request_count: Literal[0]
    provider_query_attempt_count: Literal[0]
    cache_read_count: Literal[0]
    market_data_file_read_count: Literal[0]
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
    network_io_allowed: Literal[False]
    cache_read_allowed: Literal[False]
    market_data_file_read_allowed: Literal[False]
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


class HistoricalPITReceiptAuthorityDecisionPack(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_"
        "historical_pit_receipt_authority_decision_pack.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "historical_pit_receipt_authority_decision_pack_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["OWNER_DECISION_REQUIRED_HISTORICAL_PIT_AUTHORITY_0_OF_5_REMEDIATED"]
    task_id: Literal["TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"]
    owner_scope: OwnerScope
    predecessor_binding: PredecessorBinding
    primary_research_window: PrimaryResearchWindow
    authority_class_policy: AuthorityClassPolicy
    decision_rows: tuple[AuthorityDecisionRow, ...]
    aggregate: AggregateDecision
    actual_counters: ActualCounters
    safety: Safety

    @model_validator(mode="after")
    def validate_frozen_decision_surface(self) -> Self:
        if self.authority_class_policy.historical_candidate_classes != _HISTORICAL_CLASSES:
            raise ValueError("historical authority class policy drifted")
        observed_window = (
            self.primary_research_window.exchange_calendar,
            self.primary_research_window.requested_start,
            self.primary_research_window.requested_end,
            self.primary_research_window.exact_session_count,
            self.primary_research_window.ordered_session_ids_lf_sha256,
        )
        if observed_window != _PRIMARY_WINDOW:
            raise ValueError("primary research window policy drifted")
        observed = tuple(
            (
                row.candidate_id,
                row.blocker_reason_code,
                row.source_family,
                row.acceptable_historical_authority_classes,
                row.coverage_contract,
                row.recommended_owner_action,
                row.endpoint_contract_correction_required,
            )
            for row in self.decision_rows
        )
        if observed != _DECISION_SURFACE:
            raise ValueError("historical PIT authority decision surface drifted")
        if tuple(row.ordinal for row in self.decision_rows) != tuple(range(1, 6)):
            raise ValueError("historical PIT authority decision order drifted")
        for row in self.decision_rows:
            if row.minimum_receipt_fields != _MINIMUM_RECEIPT_FIELDS[row.candidate_id]:
                raise ValueError(f"minimum receipt contract drifted: {row.candidate_id}")
        if any(row.forward_only_impact != _FORWARD_ONLY_IMPACT for row in self.decision_rows):
            raise ValueError("forward-only impact drifted")
        return self


@dataclass(frozen=True)
class HistoricalPITReceiptAuthorityDecisionPackLoadResult:
    policy: HistoricalPITReceiptAuthorityDecisionPack
    path: Path
    file_sha256: str
    canonical_sha256: str
    s9_gate: s9.MandatoryVetoManifestReplayGateLoadResult
    terminal: Literal[
        "OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED"
    ]


def load_historical_pit_receipt_authority_decision_pack(
    *,
    path: Path = DEFAULT_HISTORICAL_PIT_RECEIPT_AUTHORITY_DECISION_PACK_PATH,
    project_root: Path = PROJECT_ROOT,
) -> HistoricalPITReceiptAuthorityDecisionPackLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="historical_pit_decision_pack")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)
        loaded_s9 = s9.load_mandatory_veto_manifest_replay_gate(project_root=project_root)
        binding = policy.predecessor_binding
        bound_s9 = _bound_file(Path(binding.path), root=project_root, field="s9_predecessor")
        if hashlib.sha256(bound_s9.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S9 predecessor file SHA-256 mismatch")
        if (
            binding.file_sha256,
            binding.canonical_sha256,
        ) != (loaded_s9.file_sha256, loaded_s9.canonical_sha256):
            raise ValueError("S9 predecessor exact identity drifted")

        s9_rows = loaded_s9.policy.source_capability_gate.rows
        predecessor_surface = tuple(
            (row.candidate_id, row.reason_code, row.endpoint) for row in s9_rows
        )
        decision_surface = tuple(
            (row.candidate_id, row.blocker_reason_code, row.current_endpoint)
            for row in policy.decision_rows
        )
        if decision_surface != predecessor_surface:
            raise ValueError("S10 decision rows do not replay the exact S9 blocker surface")

        inventory = loaded_s9.policy.exact_session_inventory
        expected_window = (
            inventory.exchange_calendar,
            inventory.requested_start,
            inventory.requested_end,
            inventory.expected_session_count,
            inventory.ordered_session_ids_lf_sha256,
        )
        observed_window = (
            policy.primary_research_window.exchange_calendar,
            policy.primary_research_window.requested_start,
            policy.primary_research_window.requested_end,
            policy.primary_research_window.exact_session_count,
            policy.primary_research_window.ordered_session_ids_lf_sha256,
        )
        if observed_window != expected_window:
            raise ValueError("S10 primary research window drifted from S9")
    except HistoricalPITReceiptAuthorityDecisionPackError:
        raise
    except s9.MandatoryVetoManifestReplayGateError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise HistoricalPITReceiptAuthorityDecisionPackError(
            "HISTORICAL_PIT_RECEIPT_AUTHORITY_DECISION_PACK_REJECTED", str(exc)
        ) from exc
    return HistoricalPITReceiptAuthorityDecisionPackLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        s9_gate=loaded_s9,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_HISTORICAL_PIT_RECEIPT_AUTHORITY_DECISION_PACK_PATH",
    "HistoricalPITReceiptAuthorityDecisionPack",
    "HistoricalPITReceiptAuthorityDecisionPackError",
    "HistoricalPITReceiptAuthorityDecisionPackLoadResult",
    "load_historical_pit_receipt_authority_decision_pack",
]
