from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_capability_discovery_review import (
    QCCapabilityDiscoveryReviewLoadResult,
    load_qc_qqq_options_capability_discovery_review,
)
from ai_trading_system.qqq_options_research.bounded_cloud_pilot import (
    QQQOptionsBoundedCloudPilotPolicyLoadResult,
    load_qc_qqq_options_bounded_cloud_pilot_policy,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH = Path(
    "config/research/qc_qqq_options_bounded_cloud_pilot_owner_review_proposal_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_AUTHORITY_IDS: tuple[str, ...] = (
    "TRADING-2480-DISCOVERY-EVIDENCE-CONTRACT",
    "TRADING-2480-DISCOVERY-EVIDENCE-LOADER",
    "TRADING-2480-DISCOVERY-EVIDENCE-RECORD",
    "TRADING-2480-DISCOVERY-REVIEW-CONTRACT",
    "TRADING-2480-DISCOVERY-REVIEW-LOADER",
    "TRADING-2480-DISCOVERY-REVIEW-RECORD",
    "TRADING-2492-BLOCKED-MODULE",
    "TRADING-2492-BLOCKED-POLICY",
)
_RANK_COMPONENTS: tuple[str, ...] = (
    "ABSOLUTE_DTE_DISTANCE",
    "ABSOLUTE_DELTA_DISTANCE",
    "RELATIVE_SPREAD",
    "NEGATIVE_OPEN_INTEREST",
    "NEGATIVE_VOLUME",
    "STABLE_SID",
)
_RESULT_MAPPING_IDS: tuple[str, ...] = (
    "logs",
    "orders_csv",
    "project_files",
    "report_pdf",
    "results_json",
    "trades_csv",
)
_BLOCKING_REASON_CODES: tuple[str, ...] = (
    "OWNER_AUTHORIZATION_NOT_GRANTED",
    "PROPOSAL_REVIEW_NOT_COMPLETED",
    "PRIOR_CAPABILITY_ADMISSION_REMAINS_BLOCKED",
    "OPTION_EVENT_DQ_PIT_NOT_EVALUATED",
)


class QQQOptionsBoundedPilotOwnerReviewContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


def _required_text(value: str, field_name: str) -> str:
    if not value or value.strip() != value or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} must be normalized non-empty text")
    return value


def _identifier(value: str, field_name: str) -> str:
    value = _required_text(value, field_name)
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a portable identifier")
    return value


def _sha256(value: str, field_name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field_name: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase 40-character Git SHA")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    normalized = value.astimezone(UTC)
    if normalized > datetime.now(UTC):
        raise ValueError(f"{field_name} cannot be in the future")
    return normalized


def _canonical_json_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _lf_sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str
    _HASH_FIELD: ClassVar[str] = "content_sha256"

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def _semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={self._HASH_FIELD})

    def _expected_content_sha256(self) -> str:
        return _canonical_sha256(self._semantic_payload())

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        if self.content_sha256 not in {_UNSEALED_SHA256, self._expected_content_sha256()}:
            raise ValueError("content_sha256 does not match canonical semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: object) -> Self:
        candidate = cls.model_validate(
            {**payload, cls._HASH_FIELD: _UNSEALED_SHA256},
            strict=True,
        )
        return candidate.model_copy(
            update={cls._HASH_FIELD: candidate._expected_content_sha256()}
        )

    def canonical_bytes(self) -> bytes:
        if self.content_sha256 == _UNSEALED_SHA256:
            raise QQQOptionsBoundedPilotOwnerReviewContractError(
                "BOUNDED_PILOT_OWNER_REVIEW_RECORD_UNSEALED",
                self.__class__.__name__,
            )
        return _canonical_json_bytes(self.model_dump(mode="json"))

    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            value = cls.model_validate_json(content, strict=True)
        except ValueError as exc:
            raise QQQOptionsBoundedPilotOwnerReviewContractError(
                "BOUNDED_PILOT_OWNER_REVIEW_RECORD_INVALID",
                f"{cls.__name__}: {exc}",
            ) from exc
        if value.content_sha256 == _UNSEALED_SHA256:
            raise QQQOptionsBoundedPilotOwnerReviewContractError(
                "BOUNDED_PILOT_OWNER_REVIEW_RECORD_UNSEALED",
                cls.__name__,
            )
        if value.canonical_bytes() != content:
            raise QQQOptionsBoundedPilotOwnerReviewContractError(
                "BOUNDED_PILOT_OWNER_REVIEW_RECORD_NONCANONICAL",
                cls.__name__,
            )
        return value


class QQQOptionsBoundedPilotProposalAuthorityBinding(_PolicyModel):
    authority_id: str
    path: str
    sha256: str

    @field_validator("authority_id")
    @classmethod
    def _validate_authority_id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        value = _required_text(value, "path")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
            raise ValueError("authority path must be portable and repository-relative")
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class QQQOptionsBoundedPilotProposalResearchWindow(_PolicyModel):
    primary_research_start: date
    requested_start: date
    requested_end: date
    expected_evaluated_start: date
    expected_evaluated_end: date
    calendar_id: Literal["XNYS"]
    role: Literal["BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"]
    dq_caveat: Literal["SINGLE_CONFIRMED_ENTITLEMENT_SESSION_NOT_RESEARCH_WINDOW"]
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]

    @model_validator(mode="after")
    def _validate_window(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        expected = date(2025, 12, 2)
        if {
            self.requested_start,
            self.requested_end,
            self.expected_evaluated_start,
            self.expected_evaluated_end,
        } != {expected}:
            raise ValueError("proposal must remain the confirmed 2025-12-02 session")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker must remain 2022-12-01")
        return self


class QQQOptionsBoundedPilotProposalPlatformScope(_PolicyModel):
    account_tier: Literal["FREE"]
    cloud_compute: Literal["Community B-MICRO"]
    project_action: Literal["CREATE_OR_MUTATE_ONE_DEDICATED_PILOT_PROJECT"]
    maximum_project_mutation_count: Literal[1]
    maximum_cloud_backtest_count: Literal[1]
    maximum_runtime_seconds: Literal[300]
    maximum_processed_data_points: Literal[250000]
    maximum_order_count: Literal[1]
    maximum_contract_quantity: Literal[1]
    ticker: Literal["QQQ"]
    equity_resolution: Literal["MINUTE"]
    option_resolution: Literal["MINUTE"]


class QQQOptionsBoundedPilotProposalSelectionScope(_PolicyModel):
    technical_direction: Literal["LONG_CALL"]
    observation_time_new_york: Literal["09:31:00"]
    minimum_dte: Literal[7]
    target_dte: Literal[14]
    maximum_dte: Literal[21]
    maximum_absolute_moneyness_deviation: Literal["0.05"]
    minimum_absolute_delta: Literal["0.30"]
    target_absolute_delta: Literal["0.40"]
    maximum_absolute_delta: Literal["0.55"]
    maximum_quote_age_seconds: Literal[60]
    maximum_relative_spread: Literal["0.20"]
    minimum_open_interest: Literal[10]
    minimum_volume: Literal[0]
    rank_components: tuple[str, ...]
    no_eligible_contract_disposition: Literal["NO_ORDER_CASH_PRESERVATION"]

    @model_validator(mode="after")
    def _validate_ranking(self) -> Self:
        if self.rank_components != _RANK_COMPONENTS:
            raise ValueError("rank components must remain exact and deterministic")
        return self


class QQQOptionsBoundedPilotProposalExecutionScope(_PolicyModel):
    submission_timing: Literal["NEXT_INDEPENDENT_MINUTE_AFTER_INTENT"]
    fill_timing: Literal["NEXT_INDEPENDENT_MINUTE_AFTER_SUBMISSION"]
    submission_latency_ms: Literal[60000]
    fill_latency_ms: Literal[60000]
    maximum_quote_age_ms: Literal[60000]
    marketable_limit_buffer_per_share_usd: Literal["0.01"]
    reality_slippage_per_share_usd: Literal["0.01"]
    zero_slippage_isolation_sensitivity_per_share_usd: Literal["0.00"]
    zero_slippage_is_reality_baseline: Literal[False]
    fee_per_contract_usd: Literal["0.65"]
    maximum_contracts_per_quote: Literal[1]
    cancel_after_ms: Literal[60000]
    partial_fill_policy: Literal["PRESERVE_PARTIAL_AND_CANCEL_REMAINDER_AFTER_TIMEOUT"]
    stale_missing_crossed_quote_disposition: Literal[
        "NO_FILL_CANCEL_CASH_PRESERVATION"
    ]


class QQQOptionsBoundedPilotProposalAccountingScope(_PolicyModel):
    approved_initial_cash_usd: Literal["100000.00"]
    premium_budget_usd: Literal["2000.00"]
    maximum_contracts_per_order: Literal[1]
    fee_buffer_per_contract_usd: Literal["1.00"]
    sell_proceeds_settlement_lag_sessions: Literal[1]
    maximum_valuation_quote_age_ms: Literal[60000]
    cost_basis_method: Literal["FIFO"]
    include_fees_in_cost_basis: Literal[True]
    cash_quantum_usd: Literal["0.01"]
    rounding_mode: Literal["ROUND_HALF_EVEN"]


class QQQOptionsBoundedPilotProposalLifecycleScope(_PolicyModel):
    pre_expiry_guard_sessions: Literal[2]
    maximum_exit_quote_age_ms: Literal[60000]
    expiry_settlement_source_policy: Literal[
        "QC_ENGINE_EVENT_WITH_LOCAL_RECONCILIATION"
    ]
    scope_violation_disposition: Literal["INVALIDATE_RUN_CASH_PRESERVATION"]
    unexpected_exercise_assignment_disposition: Literal[
        "INVALIDATE_RUN_NO_UNDERLYING_DELIVERY"
    ]
    corporate_action_disposition: Literal[
        "INVALIDATE_RUN_NO_ADJUSTMENT_INFERENCE"
    ]


class QQQOptionsBoundedPilotProposalReconciliationScope(_PolicyModel):
    monetary_tolerance_usd: Literal["0.01"]
    price_tolerance_usd: Literal["0.01"]
    timestamp_tolerance_seconds: Literal[60]
    exact_order_fill_contract_identity_required: Literal[True]
    unexplained_difference_disposition: Literal[
        "PILOT_REQUIRES_REVIEW_NO_RANGE_EXPANSION"
    ]


class QQQOptionsBoundedPilotProposalEvidenceScope(_PolicyModel):
    collector_id: str
    independent_reviewer_id: str
    two_person_attestation_required: Literal[True]
    manual_bundle_required: Literal[True]
    result_mapping_ids: tuple[str, ...]
    aggregate_export_safe_artifacts_only: Literal[True]
    raw_option_rows_allowed: Literal[False]

    @field_validator("collector_id", "independent_reviewer_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_review_boundary(self) -> Self:
        if self.collector_id == self.independent_reviewer_id:
            raise ValueError("collector and independent reviewer must differ")
        if self.result_mapping_ids != _RESULT_MAPPING_IDS:
            raise ValueError("result mapping inventory must remain exact and sorted")
        return self


class QQQOptionsBoundedPilotProposalSafety(_PolicyModel):
    proposal_only: Literal[True]
    pilot_authorized: Literal[False]
    external_platform_action_allowed: Literal[False]
    project_mutation_allowed: Literal[False]
    cloud_run_allowed: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    remote_http_allowed: Literal[False]
    object_store_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    selection_activated: Literal[False]
    execution_activated: Literal[False]
    accounting_activated: Literal[False]
    lifecycle_activated: Literal[False]
    order_creation_allowed: Literal[False]
    fill_creation_allowed: Literal[False]
    cash_preservation_required: Literal[True]
    investment_interpretation_allowed: Literal[False]
    range_expansion_allowed: Literal[False]
    paper_shadow_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsBoundedPilotOwnerReviewProposal(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_bounded_cloud_pilot_owner_review_proposal.v1"
    ]
    proposal_id: Literal[
        "qc_qqq_options_bounded_cloud_pilot_owner_review_proposal_v1"
    ]
    proposal_version: Literal["1.0.0"]
    status: Literal["PROPOSED_OWNER_REVIEW_REQUIRED"]
    owner: Literal["project_owner"]
    prepared_by: Literal["codex_pilot_coordinator"]
    owner_instruction: Literal["OWNER_ALLOWED_COORDINATOR_TO_SELECT_PROPOSED_VALUES"]
    owner_authorization_token: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_REVIEW_REQUIRED_NO_EXTERNAL_ACTION"]
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    revocation_condition: str
    proposal_expires_on: date
    pilot_role: Literal["BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"]
    authority_bindings: tuple[QQQOptionsBoundedPilotProposalAuthorityBinding, ...]
    research_window: QQQOptionsBoundedPilotProposalResearchWindow
    platform_scope: QQQOptionsBoundedPilotProposalPlatformScope
    selection_scope: QQQOptionsBoundedPilotProposalSelectionScope
    execution_scope: QQQOptionsBoundedPilotProposalExecutionScope
    accounting_scope: QQQOptionsBoundedPilotProposalAccountingScope
    lifecycle_scope: QQQOptionsBoundedPilotProposalLifecycleScope
    reconciliation_scope: QQQOptionsBoundedPilotProposalReconciliationScope
    evidence_scope: QQQOptionsBoundedPilotProposalEvidenceScope
    safety: QQQOptionsBoundedPilotProposalSafety

    @field_validator(
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
        "revocation_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authority_and_expiry(self) -> Self:
        ids = tuple(item.authority_id for item in self.authority_bindings)
        if ids != _AUTHORITY_IDS or len(ids) != len(set(ids)):
            raise ValueError("proposal authority inventory must remain exact and sorted")
        if self.proposal_expires_on != date(2026, 8, 12):
            raise ValueError("proposal expiry must remain 2026-08-12")
        return self


@dataclass(frozen=True)
class QQQOptionsBoundedPilotOwnerReviewProposalLoadResult:
    proposal: QQQOptionsBoundedPilotOwnerReviewProposal
    proposal_path: Path
    proposal_policy_sha256: str
    authority_set_sha256: str
    blocked_policy: QQQOptionsBoundedCloudPilotPolicyLoadResult
    capability_review: QCCapabilityDiscoveryReviewLoadResult


class QQQOptionsBoundedPilotOwnerReviewPack(_SealedModel):
    schema_version: Literal["qc_qqq_options_bounded_cloud_pilot_owner_review_pack.v1"]
    pack_id: str
    created_at_utc: datetime
    repository_code_sha: str
    proposal_id: Literal[
        "qc_qqq_options_bounded_cloud_pilot_owner_review_proposal_v1"
    ]
    proposal_version: Literal["1.0.0"]
    proposal_policy_sha256: str
    proposal_authority_set_sha256: str
    proposal_scope_sha256: str
    blocked_2492_policy_sha256: str
    blocked_2492_authority_set_sha256: str
    capability_review_file_sha256: str
    capability_review_content_sha256: str
    capability_evidence_file_sha256: str
    capability_review_decision: Literal[
        "ACCEPTED_WITH_DISCLOSED_POST_TERMINAL_ARTIFACT_DOWNLOAD"
    ]
    prior_capability_admission_decision: Literal["CAPABILITY_OR_LICENSE_BLOCKED"]
    bounded_pilot_preparation_allowed_by_capability_review: Literal[False]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    owner_authorization_token: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_REVIEW_REQUIRED_NO_EXTERNAL_ACTION"]
    blocking_reason_codes: tuple[str, ...]
    cash_preservation_required: Literal[True]
    order_count: Literal[0]
    fill_count: Literal[0]
    external_action_executed: Literal[False]
    pilot_authorized: Literal[False]
    range_expansion_allowed: Literal[False]
    proposal_expires_on: date
    safety: QQQOptionsBoundedPilotProposalSafety

    @field_validator("pack_id")
    @classmethod
    def _validate_pack_id(cls, value: str) -> str:
        return _identifier(value, "pack_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_code_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "proposal_policy_sha256",
        "proposal_authority_set_sha256",
        "proposal_scope_sha256",
        "blocked_2492_policy_sha256",
        "blocked_2492_authority_set_sha256",
        "capability_review_file_sha256",
        "capability_review_content_sha256",
        "capability_evidence_file_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_blockers(self) -> Self:
        if self.blocking_reason_codes != _BLOCKING_REASON_CODES:
            raise ValueError("owner-review blocking reasons must remain exact")
        if self.proposal_expires_on != date(2026, 8, 12):
            raise ValueError("owner-review pack expiry mismatch")
        return self


def load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsBoundedPilotOwnerReviewProposalLoadResult:
    resolved_root = project_root.resolve()
    resolved = path if path.is_absolute() else resolved_root / path
    try:
        resolved = _require_bound_regular_file(
            path,
            project_root=resolved_root,
            field="bounded-pilot owner-review proposal",
        )
        payload = load_strict_yaml_text(
            resolved.read_text(encoding="utf-8"),
            label="bounded-pilot owner-review proposal",
        )
        if not isinstance(payload, dict):
            raise TypeError("proposal root must be a mapping")
        proposal = QQQOptionsBoundedPilotOwnerReviewProposal.model_validate(
            payload,
            strict=False,
        )
        _verify_authority_bindings(proposal, project_root=resolved_root)
        blocked_policy = load_qc_qqq_options_bounded_cloud_pilot_policy(
            project_root=resolved_root
        )
        capability_review = load_qc_qqq_options_capability_discovery_review(
            project_root=resolved_root
        )
        _verify_live_facts(
            proposal,
            blocked_policy=blocked_policy,
            capability_review=capability_review,
        )
    except QQQOptionsBoundedPilotOwnerReviewContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsBoundedPilotOwnerReviewContractError(
            "BOUNDED_PILOT_OWNER_REVIEW_PROPOSAL_INVALID",
            f"{resolved}: {exc}",
        ) from exc

    authority_set_sha256 = _canonical_sha256(
        [item.model_dump(mode="json") for item in proposal.authority_bindings]
    )
    return QQQOptionsBoundedPilotOwnerReviewProposalLoadResult(
        proposal=proposal,
        proposal_path=resolved,
        proposal_policy_sha256=_lf_sha256_path(resolved),
        authority_set_sha256=authority_set_sha256,
        blocked_policy=blocked_policy,
        capability_review=capability_review,
    )


def build_qc_qqq_options_bounded_cloud_pilot_owner_review_pack(
    *,
    pack_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    proposal_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH
    ),
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsBoundedPilotOwnerReviewPack:
    loaded = load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal(
        proposal_path,
        project_root=project_root,
    )
    proposal = loaded.proposal
    review = loaded.capability_review.review
    scope_payload = {
        "research_window": proposal.research_window.model_dump(mode="json"),
        "platform_scope": proposal.platform_scope.model_dump(mode="json"),
        "selection_scope": proposal.selection_scope.model_dump(mode="json"),
        "execution_scope": proposal.execution_scope.model_dump(mode="json"),
        "accounting_scope": proposal.accounting_scope.model_dump(mode="json"),
        "lifecycle_scope": proposal.lifecycle_scope.model_dump(mode="json"),
        "reconciliation_scope": proposal.reconciliation_scope.model_dump(mode="json"),
        "evidence_scope": proposal.evidence_scope.model_dump(mode="json"),
    }
    return QQQOptionsBoundedPilotOwnerReviewPack.seal(
        schema_version="qc_qqq_options_bounded_cloud_pilot_owner_review_pack.v1",
        pack_id=pack_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        proposal_id=proposal.proposal_id,
        proposal_version=proposal.proposal_version,
        proposal_policy_sha256=loaded.proposal_policy_sha256,
        proposal_authority_set_sha256=loaded.authority_set_sha256,
        proposal_scope_sha256=_canonical_sha256(scope_payload),
        blocked_2492_policy_sha256=loaded.blocked_policy.policy_sha256,
        blocked_2492_authority_set_sha256=(
            loaded.blocked_policy.authority_set_sha256
        ),
        capability_review_file_sha256=loaded.capability_review.review_file_sha256,
        capability_review_content_sha256=review.content_sha256,
        capability_evidence_file_sha256=review.evidence_file_sha256,
        capability_review_decision=review.review_decision,
        prior_capability_admission_decision=review.prior_admission_decision,
        bounded_pilot_preparation_allowed_by_capability_review=(
            review.bounded_pilot_preparation_allowed
        ),
        option_event_dq_status=review.option_event_dq_status,
        option_event_pit_status=review.option_event_pit_status,
        owner_authorization_token=proposal.owner_authorization_token,
        decision=proposal.decision,
        blocking_reason_codes=_BLOCKING_REASON_CODES,
        cash_preservation_required=True,
        order_count=0,
        fill_count=0,
        external_action_executed=False,
        pilot_authorized=False,
        range_expansion_allowed=False,
        proposal_expires_on=proposal.proposal_expires_on,
        safety=proposal.safety,
    )


def _require_bound_regular_file(
    path: Path,
    *,
    project_root: Path,
    field: str,
) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the project root")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    if not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} resolves outside the project root") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    return resolved


def _verify_authority_bindings(
    proposal: QQQOptionsBoundedPilotOwnerReviewProposal,
    *,
    project_root: Path,
) -> None:
    for binding in proposal.authority_bindings:
        path = _require_bound_regular_file(
            Path(binding.path),
            project_root=project_root,
            field=f"proposal authority {binding.authority_id}",
        )
        if _lf_sha256_path(path) != binding.sha256:
            raise ValueError(f"authority hash drifted: {binding.authority_id}")


def _binding_hash(
    proposal: QQQOptionsBoundedPilotOwnerReviewProposal,
    authority_id: str,
) -> str:
    return next(
        item.sha256
        for item in proposal.authority_bindings
        if item.authority_id == authority_id
    )


def _verify_live_facts(
    proposal: QQQOptionsBoundedPilotOwnerReviewProposal,
    *,
    blocked_policy: QQQOptionsBoundedCloudPilotPolicyLoadResult,
    capability_review: QCCapabilityDiscoveryReviewLoadResult,
) -> None:
    review = capability_review.review
    if blocked_policy.policy.status != "BLOCKED_OWNER_INPUT":
        raise ValueError("2492 predecessor policy must remain blocked")
    if blocked_policy.policy.owner_authorization_token != (
        "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    ):
        raise ValueError("2492 predecessor authorization token must remain not granted")
    if _binding_hash(proposal, "TRADING-2492-BLOCKED-POLICY") != (
        blocked_policy.policy_sha256
    ):
        raise ValueError("2492 blocked policy binding mismatch")
    if _binding_hash(proposal, "TRADING-2480-DISCOVERY-REVIEW-RECORD") != (
        capability_review.review_file_sha256
    ):
        raise ValueError("2480 accepted review file binding mismatch")
    if _binding_hash(proposal, "TRADING-2480-DISCOVERY-EVIDENCE-RECORD") != (
        review.evidence_file_sha256
    ):
        raise ValueError("2480 accepted evidence file binding mismatch")
    if review.review_decision != (
        "ACCEPTED_WITH_DISCLOSED_POST_TERMINAL_ARTIFACT_DOWNLOAD"
    ):
        raise ValueError("2480 review decision is not the accepted bounded evidence state")
    if review.bounded_pilot_preparation_allowed:
        raise ValueError("2480 review must not be reinterpreted as pilot preparation admission")
    if review.prior_admission_decision != "CAPABILITY_OR_LICENSE_BLOCKED":
        raise ValueError("prior capability admission must remain blocked")
    if review.option_event_dq_status != "NOT_EVALUATED":
        raise ValueError("option-event DQ must remain NOT_EVALUATED")
    if review.option_event_pit_status != "NOT_EVALUATED":
        raise ValueError("option-event PIT must remain NOT_EVALUATED")
    if review.safety.selection_or_pilot_activated:
        raise ValueError("accepted discovery review cannot activate selection or pilot")
    if proposal.platform_scope.account_tier != review.page_assertions.account_tier:
        raise ValueError("proposal tier differs from accepted discovery review")
    if proposal.platform_scope.cloud_compute != review.page_assertions.cloud_compute:
        raise ValueError("proposal compute differs from accepted discovery review")
    if proposal.research_window.requested_start != review.review_artifact.evaluated_start:
        raise ValueError("proposal start differs from confirmed entitlement session")
    if proposal.research_window.requested_end != review.review_artifact.evaluated_end:
        raise ValueError("proposal end differs from confirmed entitlement session")


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH",
    "QQQOptionsBoundedPilotOwnerReviewContractError",
    "QQQOptionsBoundedPilotOwnerReviewPack",
    "QQQOptionsBoundedPilotOwnerReviewProposal",
    "QQQOptionsBoundedPilotOwnerReviewProposalLoadResult",
    "QQQOptionsBoundedPilotProposalAccountingScope",
    "QQQOptionsBoundedPilotProposalAuthorityBinding",
    "QQQOptionsBoundedPilotProposalEvidenceScope",
    "QQQOptionsBoundedPilotProposalExecutionScope",
    "QQQOptionsBoundedPilotProposalLifecycleScope",
    "QQQOptionsBoundedPilotProposalPlatformScope",
    "QQQOptionsBoundedPilotProposalReconciliationScope",
    "QQQOptionsBoundedPilotProposalResearchWindow",
    "QQQOptionsBoundedPilotProposalSafety",
    "QQQOptionsBoundedPilotProposalSelectionScope",
    "build_qc_qqq_options_bounded_cloud_pilot_owner_review_pack",
    "load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal",
]
