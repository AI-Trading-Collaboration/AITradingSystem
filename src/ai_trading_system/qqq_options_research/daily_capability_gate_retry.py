from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.daily_capability_gate import (
    DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH,
    EXPECTED_AGGREGATE_FIELDS,
    EXPECTED_SESSIONS,
    QCQQQOptionsDailyCapabilityGateProposal,
    load_qc_qqq_options_daily_capability_gate_proposal,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH = Path(
    "config/research/qc_qqq_options_daily_capability_gate_retry_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_PROPOSAL_PATH = Path(
    "inputs/external_validation/qc_qqq_options_daily_capability_gate_retry_proposal_20260808.json"
)

EXPECTED_RETRY_ALLOWED_ACTIONS: tuple[str, ...] = (
    "QUANTCONNECT_LOGIN",
    "READ_ONLY_ACCOUNT_PROJECT_CODE_PRECHECK",
    "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
    "EXPORT_SAFE_MANUAL_EVIDENCE_COLLECTION",
)
EXPECTED_RETRY_PROHIBITED_ACTIONS: tuple[str, ...] = (
    "PROJECT_MUTATION",
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
PROPOSED_RETRY_OWNER_DECISION = (
    "owner_decision:TRADING-2500:2026-08-08:"
    "authorize_single_zero_order_verified_account_qc_daily_capability_retry_v1"
)

PREDECESSOR_PROPOSAL_FILE_SHA256 = (
    "6b226751453bc2d73e0e5ec14be6975124e3a0948435ff7282658a3c2fe3e5dc"
)
PREDECESSOR_PROPOSAL_CONTENT_SHA256 = (
    "98566866892b081ad1011e7388348c780e506018e94d568f83b1fcef888a7f95"
)
PREDECESSOR_POLICY_CANONICAL_SHA256 = (
    "1ec345fdf36a101023eacaff6ca78450bd54b45290758438f0ae4a56b2ff63f9"
)
PREDECESSOR_SCRIPT_LF_SHA256 = "1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139"
PREDECESSOR_BLOCKED_SCREENSHOT_SHA256 = (
    "c09620fa797936ab66cc0f757d3a46cca080bdcd67171474815fe3ac53ad2912"
)
PREDECESSOR_BLOCKED_BUILD_ID = "7edc98-0a3a57"

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_UNSEALED_SHA256 = "0" * 64


class QCQQQOptionsDailyCapabilityGateRetryContractError(ValueError):
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


class DailyCapabilityRetryRunScope(_PolicyModel):
    target_project_id: Literal[34808569]
    required_project_code_lf_sha256: str
    requested_start: date
    requested_end: date
    expected_sessions: tuple[date, ...]
    ticker: Literal["QQQ"]
    equity_resolution: Literal["DAILY"]
    equity_normalization: Literal["RAW"]
    option_resolution: Literal["DAILY"]
    capability_only: Literal[True]
    account_verification_precheck_required: Literal[True]
    maximum_project_mutations: Literal[0]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    raw_rows_logged: Literal[False]

    @field_validator("required_project_code_lf_sha256")
    @classmethod
    def _validate_code_hash(cls, value: str) -> str:
        if value != PREDECESSOR_SCRIPT_LF_SHA256:
            raise ValueError("required project code identity drifted")
        return _sha256(value, "required_project_code_lf_sha256")

    @model_validator(mode="after")
    def _validate_range(self) -> Self:
        if self.requested_start != EXPECTED_SESSIONS[0]:
            raise ValueError("requested_start must be the primary-window start")
        if self.requested_end != EXPECTED_SESSIONS[-1]:
            raise ValueError("requested_end must remain 2021-02-26")
        if self.expected_sessions != EXPECTED_SESSIONS:
            raise ValueError("expected session inventory drifted")
        return self


class DailyCapabilityRetrySafety(_PolicyModel):
    proposal_only: Literal[True]
    owner_signature_present: Literal[False]
    external_platform_action_authorized: Literal[False]
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


class QCQQQOptionsDailyCapabilityGateRetryPolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_daily_capability_gate_retry_policy.v1"]
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
    predecessor_script_lf_sha256: str
    predecessor_blocked_build_id: str
    predecessor_blocked_screenshot_sha256: str
    predecessor_disposition: Literal[
        "NO_GO_CAPABILITY_OR_ENTITLEMENT_ACCOUNT_VERIFICATION_REQUIRED"
    ]
    predecessor_authorization_status: Literal["INVALIDATED_SINGLE_USE"]
    predecessor_backtest_id_present: Literal[False]
    predecessor_independent_review_status: Literal["PENDING_INDEPENDENT_REVIEW"]
    account_verification_claim_status: Literal["OWNER_CLAIMED_REQUIRES_UI_CONFIRMATION"]
    run_scope: DailyCapabilityRetryRunScope
    required_aggregate_fields: tuple[str, ...]
    allowed_actions_after_exact_owner_token: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    proposed_owner_decision: str
    successor_task_id: Literal["TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1"]
    safety: DailyCapabilityRetrySafety

    @field_validator("policy_id")
    @classmethod
    def _validate_policy_id(cls, value: str) -> str:
        return _identifier(value, "policy_id")

    @field_validator("rationale", "predecessor_proposal_relative_path")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("predecessor_blocked_build_id")
    @classmethod
    def _validate_build_id(cls, value: str) -> str:
        if value != PREDECESSOR_BLOCKED_BUILD_ID:
            raise ValueError("predecessor build identity drifted")
        return _required_text(value, "predecessor_blocked_build_id")

    @field_validator(
        "predecessor_proposal_file_sha256",
        "predecessor_proposal_content_sha256",
        "predecessor_policy_canonical_sha256",
        "predecessor_script_lf_sha256",
        "predecessor_blocked_screenshot_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: Any) -> str:
        expected = {
            "predecessor_proposal_file_sha256": PREDECESSOR_PROPOSAL_FILE_SHA256,
            "predecessor_proposal_content_sha256": PREDECESSOR_PROPOSAL_CONTENT_SHA256,
            "predecessor_policy_canonical_sha256": PREDECESSOR_POLICY_CANONICAL_SHA256,
            "predecessor_script_lf_sha256": PREDECESSOR_SCRIPT_LF_SHA256,
            "predecessor_blocked_screenshot_sha256": (PREDECESSOR_BLOCKED_SCREENSHOT_SHA256),
        }[str(info.field_name)]
        if value != expected:
            raise ValueError(f"{info.field_name} drifted")
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.review_on != date(2026, 8, 8) or self.expires_on != date(2026, 8, 15):
            raise ValueError("review/expiry boundary drifted")
        if self.required_aggregate_fields != EXPECTED_AGGREGATE_FIELDS:
            raise ValueError("required aggregate field inventory drifted")
        if self.allowed_actions_after_exact_owner_token != EXPECTED_RETRY_ALLOWED_ACTIONS:
            raise ValueError("allowed action inventory drifted")
        if self.prohibited_actions != EXPECTED_RETRY_PROHIBITED_ACTIONS:
            raise ValueError("prohibited action inventory drifted")
        if self.proposed_owner_decision != PROPOSED_RETRY_OWNER_DECISION:
            raise ValueError("proposed Owner decision drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QCQQQOptionsDailyCapabilityGateRetryPolicyLoadResult:
    policy: QCQQQOptionsDailyCapabilityGateRetryPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    predecessor_proposal_path: Path
    predecessor: QCQQQOptionsDailyCapabilityGateProposal


class QCQQQOptionsDailyCapabilityGateRetryProposal(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_capability_gate_retry_proposal.v1"]
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
    predecessor_script_lf_sha256: str
    predecessor_blocked_build_id: str
    predecessor_blocked_screenshot_sha256: str
    predecessor_disposition: Literal[
        "NO_GO_CAPABILITY_OR_ENTITLEMENT_ACCOUNT_VERIFICATION_REQUIRED"
    ]
    predecessor_authorization_status: Literal["INVALIDATED_SINGLE_USE"]
    predecessor_backtest_id_present: Literal[False]
    predecessor_independent_review_status: Literal["PENDING_INDEPENDENT_REVIEW"]
    account_verification_claim_status: Literal["OWNER_CLAIMED_REQUIRES_UI_CONFIRMATION"]
    run_scope: DailyCapabilityRetryRunScope
    required_aggregate_fields: tuple[str, ...]
    allowed_actions_after_exact_owner_token: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    proposed_owner_decision: str
    successor_task_id: Literal["TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1"]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    owner_token_present: Literal[False]
    gate_status: Literal["UNKNOWN_EVIDENCE_INCOMPLETE"]
    gate_reason: Literal["NEW_OWNER_TOKEN_AND_RETRY_EVIDENCE_REQUIRED"]
    safety: DailyCapabilityRetrySafety

    @field_validator("record_id", "policy_id")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at_utc must be timezone-aware")
        checked = value.astimezone(UTC)
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
        "predecessor_script_lf_sha256",
        "predecessor_blocked_screenshot_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_proposal(self) -> Self:
        if self.required_aggregate_fields != EXPECTED_AGGREGATE_FIELDS:
            raise ValueError("proposal aggregate field inventory drifted")
        if self.allowed_actions_after_exact_owner_token != EXPECTED_RETRY_ALLOWED_ACTIONS:
            raise ValueError("proposal allowed action inventory drifted")
        if self.prohibited_actions != EXPECTED_RETRY_PROHIBITED_ACTIONS:
            raise ValueError("proposal prohibited action inventory drifted")
        if self.proposed_owner_decision != PROPOSED_RETRY_OWNER_DECISION:
            raise ValueError("proposed Owner decision drifted")
        return self


@dataclass(frozen=True)
class QCQQQOptionsDailyCapabilityGateRetryProposalLoadResult:
    proposal: QCQQQOptionsDailyCapabilityGateRetryProposal
    proposal_path: Path
    proposal_file_sha256: str
    policy: QCQQQOptionsDailyCapabilityGateRetryPolicyLoadResult


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


def load_qc_qqq_options_daily_capability_gate_retry_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateRetryPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="daily capability retry policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("daily capability retry policy root must be a mapping")
        policy = QCQQQOptionsDailyCapabilityGateRetryPolicy.model_validate(payload)
        predecessor_path = _require_bound_regular_file(
            Path(policy.predecessor_proposal_relative_path),
            project_root=root,
            field="TRADING-2498 daily capability proposal",
        )
        loaded_predecessor = load_qc_qqq_options_daily_capability_gate_proposal(
            predecessor_path,
            policy_path=DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH,
            project_root=root,
        )
        predecessor = loaded_predecessor.proposal
        if loaded_predecessor.proposal_file_sha256 != policy.predecessor_proposal_file_sha256:
            raise ValueError("TRADING-2498 proposal file SHA-256 mismatch")
        if predecessor.content_sha256 != policy.predecessor_proposal_content_sha256:
            raise ValueError("TRADING-2498 proposal content SHA-256 mismatch")
        if (
            loaded_predecessor.policy.policy_canonical_sha256
            != policy.predecessor_policy_canonical_sha256
        ):
            raise ValueError("TRADING-2498 policy canonical SHA-256 mismatch")
        if (
            predecessor.authorization_status != "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
            or predecessor.owner_token_present
            or predecessor.gate_status != "UNKNOWN_EVIDENCE_INCOMPLETE"
        ):
            raise ValueError("TRADING-2498 tracked proposal safety boundary drifted")
    except QCQQQOptionsDailyCapabilityGateRetryContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateRetryContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsDailyCapabilityGateRetryPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        predecessor_proposal_path=predecessor_path,
        predecessor=predecessor,
    )


def build_qc_qqq_options_daily_capability_gate_retry_proposal(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateRetryProposal:
    loaded = load_qc_qqq_options_daily_capability_gate_retry_policy(
        policy_path, project_root=project_root
    )
    policy = loaded.policy
    authority_set_sha256 = _canonical_sha256(
        {
            "repository_code_sha": repository_code_sha,
            "policy_file_sha256": loaded.policy_file_sha256,
            "policy_canonical_sha256": loaded.policy_canonical_sha256,
            "predecessor_proposal_file_sha256": policy.predecessor_proposal_file_sha256,
            "predecessor_proposal_content_sha256": policy.predecessor_proposal_content_sha256,
            "predecessor_policy_canonical_sha256": policy.predecessor_policy_canonical_sha256,
            "predecessor_script_lf_sha256": policy.predecessor_script_lf_sha256,
            "predecessor_blocked_build_id": policy.predecessor_blocked_build_id,
            "predecessor_blocked_screenshot_sha256": (policy.predecessor_blocked_screenshot_sha256),
            "run_scope": policy.run_scope.model_dump(mode="json"),
            "required_aggregate_fields": policy.required_aggregate_fields,
            "allowed_actions_after_exact_owner_token": (
                policy.allowed_actions_after_exact_owner_token
            ),
            "prohibited_actions": policy.prohibited_actions,
        }
    )
    return QCQQQOptionsDailyCapabilityGateRetryProposal.seal(
        schema_version="qc_qqq_options_daily_capability_gate_retry_proposal.v1",
        record_id=record_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        authority_set_sha256=authority_set_sha256,
        predecessor_proposal_file_sha256=policy.predecessor_proposal_file_sha256,
        predecessor_proposal_content_sha256=policy.predecessor_proposal_content_sha256,
        predecessor_policy_canonical_sha256=policy.predecessor_policy_canonical_sha256,
        predecessor_script_lf_sha256=policy.predecessor_script_lf_sha256,
        predecessor_blocked_build_id=policy.predecessor_blocked_build_id,
        predecessor_blocked_screenshot_sha256=(policy.predecessor_blocked_screenshot_sha256),
        predecessor_disposition=policy.predecessor_disposition,
        predecessor_authorization_status=policy.predecessor_authorization_status,
        predecessor_backtest_id_present=policy.predecessor_backtest_id_present,
        predecessor_independent_review_status=(policy.predecessor_independent_review_status),
        account_verification_claim_status=policy.account_verification_claim_status,
        run_scope=policy.run_scope,
        required_aggregate_fields=policy.required_aggregate_fields,
        allowed_actions_after_exact_owner_token=policy.allowed_actions_after_exact_owner_token,
        prohibited_actions=policy.prohibited_actions,
        proposed_owner_decision=policy.proposed_owner_decision,
        successor_task_id=policy.successor_task_id,
        authorization_status="NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
        owner_token_present=False,
        gate_status="UNKNOWN_EVIDENCE_INCOMPLETE",
        gate_reason="NEW_OWNER_TOKEN_AND_RETRY_EVIDENCE_REQUIRED",
        safety=policy.safety,
    )


def load_qc_qqq_options_daily_capability_gate_retry_proposal(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_PROPOSAL_PATH,
    *,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateRetryProposalLoadResult:
    root = project_root.resolve()
    try:
        proposal_path = _require_bound_regular_file(
            path, project_root=root, field="daily capability retry proposal"
        )
        raw = proposal_path.read_bytes()
        proposal = QCQQQOptionsDailyCapabilityGateRetryProposal.from_json_bytes(raw)
        expected = build_qc_qqq_options_daily_capability_gate_retry_proposal(
            record_id=proposal.record_id,
            created_at_utc=proposal.created_at_utc,
            repository_code_sha=proposal.repository_code_sha,
            policy_path=policy_path,
            project_root=root,
        )
        if proposal != expected:
            raise ValueError("retry proposal does not replay from frozen policy authority")
        policy = load_qc_qqq_options_daily_capability_gate_retry_policy(
            policy_path, project_root=root
        )
    except QCQQQOptionsDailyCapabilityGateRetryContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateRetryContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_PROPOSAL_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsDailyCapabilityGateRetryProposalLoadResult(
        proposal=proposal,
        proposal_path=proposal_path,
        proposal_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy=policy,
    )
