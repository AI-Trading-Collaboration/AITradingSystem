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
from ai_trading_system.qqq_options_research.license_export_owner_review import (
    DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
    QCQQQOptionsLicenseExportOwnerReviewProposal,
    load_qc_qqq_options_license_export_owner_review_proposal,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH = Path(
    "config/research/qc_qqq_options_daily_capability_gate_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_PROPOSAL_PATH = Path(
    "inputs/external_validation/qc_qqq_options_daily_capability_gate_proposal_20260808.json"
)

EXPECTED_SESSIONS: tuple[date, ...] = (
    date(2021, 2, 22),
    date(2021, 2, 23),
    date(2021, 2, 24),
    date(2021, 2, 25),
    date(2021, 2, 26),
)
EXPECTED_AGGREGATE_FIELDS: tuple[str, ...] = (
    "option_chain_present",
    "contract_count",
    "two_sided_quote_count",
    "positive_open_interest_count",
    "finite_greeks_count",
    "finite_implied_volatility_count",
    "raw_rows_logged",
    "orders_submitted",
)
EXPECTED_ALLOWED_ACTIONS: tuple[str, ...] = (
    "QUANTCONNECT_LOGIN",
    "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE",
    "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
    "EXPORT_SAFE_MANUAL_EVIDENCE_COLLECTION",
)
EXPECTED_PROHIBITED_ACTIONS: tuple[str, ...] = (
    "CREATE_SECOND_PROJECT",
    "SECOND_CLOUD_BACKTEST",
    "SUBMIT_ORDER",
    "API",
    "CLI",
    "HTTP",
    "OBJECT_STORE",
    "RAW_OPTIONS_DATA_DOWNLOAD",
    "RAW_OPTION_ROW_LOGGING_OR_EXPORT",
    "PURCHASE_OR_SUBSCRIPTION",
    "RANGE_EXPANSION",
    "INVESTMENT_INTERPRETATION",
    "PAPER",
    "LIVE",
    "BROKER",
    "PRODUCTION",
)
PROPOSED_OWNER_DECISION = (
    "owner_decision:TRADING-2498:2026-08-08:authorize_single_zero_order_qc_daily_capability_gate_v1"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_UNSEALED_SHA256 = "0" * 64


class DailyCapabilityGateDecision(StrEnum):
    GO_FOR_DAILY_ENGINEERING_ONLY = "GO_FOR_DAILY_ENGINEERING_ONLY"
    NO_GO_CAPABILITY_OR_ENTITLEMENT = "NO_GO_CAPABILITY_OR_ENTITLEMENT"
    UNKNOWN_EVIDENCE_INCOMPLETE = "UNKNOWN_EVIDENCE_INCOMPLETE"


class QCQQQOptionsDailyCapabilityGateContractError(ValueError):
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
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
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


class DailyCapabilityRunScope(_PolicyModel):
    target_project_id: int
    requested_start: date
    requested_end: date
    expected_sessions: tuple[date, ...]
    ticker: Literal["QQQ"]
    equity_resolution: Literal["DAILY"]
    equity_normalization: Literal["RAW"]
    option_resolution: Literal["DAILY"]
    capability_only: Literal[True]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    raw_rows_logged: Literal[False]

    @field_validator("target_project_id")
    @classmethod
    def _validate_project_id(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("target_project_id must be positive")
        return value

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        if self.requested_start != EXPECTED_SESSIONS[0]:
            raise ValueError("requested_start must be the primary-window start")
        if self.requested_end != EXPECTED_SESSIONS[-1]:
            raise ValueError("requested_end must remain 2021-02-26")
        if self.expected_sessions != EXPECTED_SESSIONS:
            raise ValueError("expected session inventory drifted")
        return self


class DailyCapabilityGateSafety(_PolicyModel):
    proposal_only: Literal[True]
    owner_signature_present: Literal[False]
    quantconnect_login_authorized: Literal[False]
    project_mutation_authorized: Literal[False]
    cloud_backtest_authorized: Literal[False]
    api_cli_http_object_store_authorized: Literal[False]
    raw_options_download_authorized: Literal[False]
    purchase_or_subscription_authorized: Literal[False]
    range_expansion_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action: Literal["none"]


class QCQQQOptionsDailyCapabilityGatePolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_daily_capability_gate_policy.v1"]
    policy_id: str
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED"]
    owner: Literal["project_owner"]
    rationale: str
    review_on: date
    expires_on: date
    predecessor_proposal_relative_path: str
    predecessor_proposal_file_sha256: str
    predecessor_proposal_content_sha256: str
    predecessor_policy_canonical_sha256: str
    predecessor_aggregate_recommendation: Literal[
        "NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES"
    ]
    run_scope: DailyCapabilityRunScope
    required_aggregate_fields: tuple[str, ...]
    allowed_actions_after_exact_owner_token: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    proposed_owner_decision: Literal[
        "owner_decision:TRADING-2498:2026-08-08:authorize_single_zero_order_qc_daily_capability_gate_v1"
    ]
    successor_task_id: Literal["TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1"]
    safety: DailyCapabilityGateSafety

    @field_validator("policy_id")
    @classmethod
    def _validate_policy_id(cls, value: str) -> str:
        return _identifier(value, "policy_id")

    @field_validator("rationale", "predecessor_proposal_relative_path")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "predecessor_proposal_file_sha256",
        "predecessor_proposal_content_sha256",
        "predecessor_policy_canonical_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.review_on != date(2026, 8, 8) or self.expires_on != date(2026, 8, 15):
            raise ValueError("review/expiry boundary drifted")
        if self.required_aggregate_fields != EXPECTED_AGGREGATE_FIELDS:
            raise ValueError("required aggregate field inventory drifted")
        if self.allowed_actions_after_exact_owner_token != EXPECTED_ALLOWED_ACTIONS:
            raise ValueError("allowed action inventory drifted")
        if self.prohibited_actions != EXPECTED_PROHIBITED_ACTIONS:
            raise ValueError("prohibited action inventory drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QCQQQOptionsDailyCapabilityGatePolicyLoadResult:
    policy: QCQQQOptionsDailyCapabilityGatePolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    predecessor_proposal_path: Path
    predecessor: QCQQQOptionsLicenseExportOwnerReviewProposal


class QCQQQOptionsDailyCapabilityGateProposal(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_capability_gate_proposal.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str
    predecessor_proposal_file_sha256: str
    predecessor_proposal_content_sha256: str
    predecessor_policy_canonical_sha256: str
    predecessor_aggregate_recommendation: Literal[
        "NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES"
    ]
    run_scope: DailyCapabilityRunScope
    required_aggregate_fields: tuple[str, ...]
    allowed_actions_after_exact_owner_token: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    proposed_owner_decision: str
    successor_task_id: Literal["TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1"]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    owner_token_present: Literal[False]
    gate_status: Literal["UNKNOWN_EVIDENCE_INCOMPLETE"]
    gate_reason: Literal["OWNER_TOKEN_AND_EXTERNAL_EVIDENCE_REQUIRED"]
    safety: DailyCapabilityGateSafety

    @field_validator("record_id", "policy_id")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        checked = _utc(value, "created_at_utc")
        if checked.date() != date(2026, 8, 8):
            raise ValueError("created_at_utc must match the proposal date")
        return checked

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "authority_set_sha256",
        "predecessor_proposal_file_sha256",
        "predecessor_proposal_content_sha256",
        "predecessor_policy_canonical_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_proposal(self) -> Self:
        if self.required_aggregate_fields != EXPECTED_AGGREGATE_FIELDS:
            raise ValueError("proposal aggregate field inventory drifted")
        if self.allowed_actions_after_exact_owner_token != EXPECTED_ALLOWED_ACTIONS:
            raise ValueError("proposal allowed actions drifted")
        if self.prohibited_actions != EXPECTED_PROHIBITED_ACTIONS:
            raise ValueError("proposal prohibited actions drifted")
        if self.proposed_owner_decision != PROPOSED_OWNER_DECISION:
            raise ValueError("proposed Owner decision drifted")
        return self


@dataclass(frozen=True)
class QCQQQOptionsDailyCapabilityGateProposalLoadResult:
    proposal: QCQQQOptionsDailyCapabilityGateProposal
    proposal_path: Path
    proposal_file_sha256: str
    policy: QCQQQOptionsDailyCapabilityGatePolicyLoadResult


class DailyCapabilitySessionObservation(_StrictModel):
    session: date
    option_chain_present: bool
    contract_count: int
    two_sided_quote_count: int
    positive_open_interest_count: int
    finite_greeks_count: int
    finite_implied_volatility_count: int
    raw_rows_logged: Literal[False]
    orders_submitted: Literal[0]

    @field_validator(
        "contract_count",
        "two_sided_quote_count",
        "positive_open_interest_count",
        "finite_greeks_count",
        "finite_implied_volatility_count",
    )
    @classmethod
    def _validate_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("aggregate counts cannot be negative")
        return value


class QCQQQOptionsDailyCapabilityRunObservation(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_capability_run_observation.v1"]
    record_id: str
    observed_at_utc: datetime
    proposal_file_sha256: str
    proposal_content_sha256: str
    authorization_token_sha256: str
    project_id: int
    backtest_id: str
    algorithm_id: str
    engine_version: str
    build_id: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    free_organization_reviewed: bool
    daily_entitlement_observed: bool
    session_observations: tuple[DailyCapabilitySessionObservation, ...]
    processed_data_points: int
    elapsed_milliseconds: int
    orders_submitted: Literal[0]
    fills: Literal[0]
    portfolio_invested: Literal[False]
    raw_rows_logged_or_exported: Literal[False]
    scope_violation_detected: bool
    result_artifact_byte_count: int
    result_artifact_sha256: str
    reviewed_by: str
    exceptions: tuple[str, ...]

    @field_validator("record_id", "backtest_id", "algorithm_id")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("engine_version", "build_id", "reviewed_by")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("observed_at_utc")
    @classmethod
    def _validate_observed_at(cls, value: datetime) -> datetime:
        return _utc(value, "observed_at_utc")

    @field_validator(
        "proposal_file_sha256",
        "proposal_content_sha256",
        "authorization_token_sha256",
        "result_artifact_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator(
        "project_id", "processed_data_points", "elapsed_milliseconds", "result_artifact_byte_count"
    )
    @classmethod
    def _validate_nonnegative(cls, value: int, info: Any) -> int:
        minimum = 1 if str(info.field_name) in {"project_id", "result_artifact_byte_count"} else 0
        if value < minimum:
            raise ValueError(f"{info.field_name} is below its protocol minimum")
        return value

    @model_validator(mode="after")
    def _validate_session_inventory(self) -> Self:
        sessions = tuple(item.session for item in self.session_observations)
        if sessions != tuple(sorted(sessions)) or len(set(sessions)) != len(sessions):
            raise ValueError("session observations must be unique and ordered")
        if any(item.session not in EXPECTED_SESSIONS for item in self.session_observations):
            raise ValueError("session observation escapes the frozen range")
        return self


class QCQQQOptionsDailyCapabilityGateRecord(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_capability_gate_record.v1"]
    record_id: str
    created_at_utc: datetime
    proposal_file_sha256: str
    proposal_content_sha256: str
    observation_content_sha256: str
    decision: DailyCapabilityGateDecision
    reason_codes: tuple[str, ...]
    engineering_successor_allowed: bool
    full_window_cloud_run_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    broker_action: Literal["none"]

    @field_validator("record_id")
    @classmethod
    def _validate_identifier(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator(
        "proposal_file_sha256", "proposal_content_sha256", "observation_content_sha256"
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("reason_codes")
    @classmethod
    def _validate_reason_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(set(value))):
            raise ValueError("reason_codes must be non-empty, unique and ordered")
        for item in value:
            _identifier(item, "reason_code")
        return value

    @model_validator(mode="after")
    def _validate_decision_binding(self) -> Self:
        expected = self.decision is DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY
        if self.engineering_successor_allowed is not expected:
            raise ValueError("successor permission does not match the gate decision")
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


def load_qc_qqq_options_daily_capability_gate_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGatePolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="daily capability gate policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("daily capability gate policy root must be a mapping")
        policy = QCQQQOptionsDailyCapabilityGatePolicy.model_validate(payload)
        predecessor_path = _require_bound_regular_file(
            Path(policy.predecessor_proposal_relative_path),
            project_root=root,
            field="TRADING-2497 Owner-review proposal",
        )
        loaded_predecessor = load_qc_qqq_options_license_export_owner_review_proposal(
            predecessor_path,
            policy_path=DEFAULT_QC_QQQ_OPTIONS_LICENSE_EXPORT_OWNER_REVIEW_POLICY_PATH,
            project_root=root,
        )
        predecessor = loaded_predecessor.proposal
        if loaded_predecessor.proposal_file_sha256 != policy.predecessor_proposal_file_sha256:
            raise ValueError("TRADING-2497 proposal file SHA-256 mismatch")
        if predecessor.content_sha256 != policy.predecessor_proposal_content_sha256:
            raise ValueError("TRADING-2497 proposal content SHA-256 mismatch")
        if (
            loaded_predecessor.policy.policy_canonical_sha256
            != policy.predecessor_policy_canonical_sha256
        ):
            raise ValueError("TRADING-2497 policy canonical SHA-256 mismatch")
        if (
            predecessor.aggregate_recommendation != policy.predecessor_aggregate_recommendation
            or predecessor.owner_review_completed
            or predecessor.owner_attestation_present
            or predecessor.primary_window_status != "NOT_TESTED_ACCOUNT_SPECIFIC"
            or predecessor.safety.external_platform_action_authorized
        ):
            raise ValueError("TRADING-2497 predecessor safety boundary drifted")
    except QCQQQOptionsDailyCapabilityGateContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsDailyCapabilityGatePolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        predecessor_proposal_path=predecessor_path,
        predecessor=predecessor,
    )


def build_qc_qqq_options_daily_capability_gate_proposal(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateProposal:
    loaded = load_qc_qqq_options_daily_capability_gate_policy(
        policy_path, project_root=project_root
    )
    policy = loaded.policy
    predecessor = loaded.predecessor
    authority_set_sha256 = _canonical_sha256(
        {
            "repository_code_sha": repository_code_sha,
            "policy_file_sha256": loaded.policy_file_sha256,
            "policy_canonical_sha256": loaded.policy_canonical_sha256,
            "predecessor_proposal_file_sha256": policy.predecessor_proposal_file_sha256,
            "predecessor_proposal_content_sha256": predecessor.content_sha256,
            "predecessor_policy_canonical_sha256": policy.predecessor_policy_canonical_sha256,
            "run_scope": policy.run_scope.model_dump(mode="json"),
            "required_aggregate_fields": policy.required_aggregate_fields,
            "allowed_actions_after_exact_owner_token": (
                policy.allowed_actions_after_exact_owner_token
            ),
            "prohibited_actions": policy.prohibited_actions,
        }
    )
    return QCQQQOptionsDailyCapabilityGateProposal.seal(
        schema_version="qc_qqq_options_daily_capability_gate_proposal.v1",
        record_id=record_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        authority_set_sha256=authority_set_sha256,
        predecessor_proposal_file_sha256=policy.predecessor_proposal_file_sha256,
        predecessor_proposal_content_sha256=predecessor.content_sha256,
        predecessor_policy_canonical_sha256=policy.predecessor_policy_canonical_sha256,
        predecessor_aggregate_recommendation=predecessor.aggregate_recommendation,
        run_scope=policy.run_scope,
        required_aggregate_fields=policy.required_aggregate_fields,
        allowed_actions_after_exact_owner_token=policy.allowed_actions_after_exact_owner_token,
        prohibited_actions=policy.prohibited_actions,
        proposed_owner_decision=policy.proposed_owner_decision,
        successor_task_id=policy.successor_task_id,
        authorization_status="NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
        owner_token_present=False,
        gate_status="UNKNOWN_EVIDENCE_INCOMPLETE",
        gate_reason="OWNER_TOKEN_AND_EXTERNAL_EVIDENCE_REQUIRED",
        safety=policy.safety,
    )


def load_qc_qqq_options_daily_capability_gate_proposal(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_PROPOSAL_PATH,
    *,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateProposalLoadResult:
    root = project_root.resolve()
    try:
        proposal_path = _require_bound_regular_file(
            path, project_root=root, field="daily capability gate proposal"
        )
        raw = proposal_path.read_bytes()
        proposal = QCQQQOptionsDailyCapabilityGateProposal.from_json_bytes(raw)
        expected = build_qc_qqq_options_daily_capability_gate_proposal(
            record_id=proposal.record_id,
            created_at_utc=proposal.created_at_utc,
            repository_code_sha=proposal.repository_code_sha,
            policy_path=policy_path,
            project_root=root,
        )
        if proposal != expected:
            raise ValueError("proposal does not replay from current frozen policy authority")
        policy = load_qc_qqq_options_daily_capability_gate_policy(policy_path, project_root=root)
    except QCQQQOptionsDailyCapabilityGateContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_PROPOSAL_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsDailyCapabilityGateProposalLoadResult(
        proposal=proposal,
        proposal_path=proposal_path,
        proposal_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy=policy,
    )


def build_qc_qqq_options_daily_capability_gate_record(
    *,
    record_id: str,
    created_at_utc: datetime,
    proposal_file_sha256: str,
    proposal: QCQQQOptionsDailyCapabilityGateProposal,
    observation: QCQQQOptionsDailyCapabilityRunObservation,
) -> QCQQQOptionsDailyCapabilityGateRecord:
    reasons: set[str] = set()
    decision = DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY

    if observation.proposal_file_sha256 != proposal_file_sha256:
        reasons.add("PROPOSAL_FILE_IDENTITY_MISMATCH")
    if observation.proposal_content_sha256 != proposal.content_sha256:
        reasons.add("PROPOSAL_CONTENT_IDENTITY_MISMATCH")
    if observation.project_id != proposal.run_scope.target_project_id:
        reasons.add("TARGET_PROJECT_MISMATCH")
    if observation.scope_violation_detected:
        reasons.add("SCOPE_VIOLATION")
    if (
        observation.requested_start != proposal.run_scope.requested_start
        or observation.requested_end != proposal.run_scope.requested_end
        or observation.evaluated_start != proposal.run_scope.requested_start
        or observation.evaluated_end != proposal.run_scope.requested_end
    ):
        reasons.add("REQUESTED_OR_EVALUATED_RANGE_MISMATCH")
    if not observation.daily_entitlement_observed:
        reasons.add("DAILY_ENTITLEMENT_NOT_OBSERVED")
    if reasons:
        decision = DailyCapabilityGateDecision.NO_GO_CAPABILITY_OR_ENTITLEMENT

    session_map = {item.session: item for item in observation.session_observations}
    if tuple(session_map) != EXPECTED_SESSIONS:
        reasons.add("EXPECTED_SESSION_EVIDENCE_INCOMPLETE")
        if decision is DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY:
            decision = DailyCapabilityGateDecision.UNKNOWN_EVIDENCE_INCOMPLETE

    for session in EXPECTED_SESSIONS:
        item = session_map.get(session)
        if item is None:
            continue
        if not item.option_chain_present or item.contract_count == 0:
            reasons.add("OPTION_CHAIN_OR_CONTRACT_MISSING")
        if item.two_sided_quote_count == 0:
            reasons.add("TWO_SIDED_QUOTE_MISSING")
        if item.positive_open_interest_count == 0:
            reasons.add("POSITIVE_OPEN_INTEREST_MISSING")
        if item.finite_greeks_count == 0 or item.finite_implied_volatility_count == 0:
            reasons.add("GREEKS_OR_IV_MISSING")

    if any(
        code in reasons
        for code in {
            "OPTION_CHAIN_OR_CONTRACT_MISSING",
            "TWO_SIDED_QUOTE_MISSING",
            "POSITIVE_OPEN_INTEREST_MISSING",
            "GREEKS_OR_IV_MISSING",
        }
    ):
        decision = DailyCapabilityGateDecision.NO_GO_CAPABILITY_OR_ENTITLEMENT
    if (
        not observation.free_organization_reviewed
        and decision is DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY
    ):
        reasons.add("FREE_ORGANIZATION_REVIEW_MISSING")
        decision = DailyCapabilityGateDecision.UNKNOWN_EVIDENCE_INCOMPLETE
    if (
        observation.exceptions
        and decision is DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY
    ):
        reasons.add("REVIEW_EXCEPTION_PRESENT")
        decision = DailyCapabilityGateDecision.UNKNOWN_EVIDENCE_INCOMPLETE
    if not reasons:
        reasons.add("ALL_FROZEN_DAILY_CAPABILITY_CHECKS_PASS")

    return QCQQQOptionsDailyCapabilityGateRecord.seal(
        schema_version="qc_qqq_options_daily_capability_gate_record.v1",
        record_id=record_id,
        created_at_utc=created_at_utc,
        proposal_file_sha256=proposal_file_sha256,
        proposal_content_sha256=proposal.content_sha256,
        observation_content_sha256=observation.content_sha256,
        decision=decision,
        reason_codes=tuple(sorted(reasons)),
        engineering_successor_allowed=(
            decision is DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY
        ),
        full_window_cloud_run_authorized=False,
        investment_interpretation_allowed=False,
        broker_action="none",
    )
