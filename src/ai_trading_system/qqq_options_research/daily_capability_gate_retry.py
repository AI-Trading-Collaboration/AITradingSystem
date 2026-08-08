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
DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_PATH = Path(
    "inputs/external_validation/qc_qqq_options_daily_capability_gate_retry_evidence_20260808.json"
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
RETRY_OWNER_AUTHORIZATION_ID = PROPOSED_RETRY_OWNER_DECISION
EXPECTED_RETRY_RESULT_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "algorithmConfiguration",
    "analysis",
    "charts",
    "orders",
    "profitLoss",
    "rollingWindow",
    "runtimeStatistics",
    "state",
    "statistics",
    "totalPerformance",
)
RETRY_PROPOSAL_REPOSITORY_CODE_SHA = "c880bb9e55dbcf5c641756e80fdd2f9d00eaa0e2"
RETRY_ORDINARY_PUSHED_MAIN_SHA = "ab22067ab9f57cc11144ae4eef899cb21f639181"
RETRY_PROPOSAL_FILE_SHA256 = "d5ecad8167e2abef7e5a8d6427604da5b6f59d4be50607228097191eba74239e"
RETRY_PROPOSAL_CONTENT_SHA256 = "77570e7ff88e1c567c29d10dcfc534cef07628cab58ceb894da79c6075f013b9"
RETRY_POLICY_FILE_SHA256 = "851ee0fb3c2a14b25263b37115ece581869fee08dffac95e272960108c46bb19"
RETRY_POLICY_CANONICAL_SHA256 = "540107c9dce0fa08a8f461f8c733a1c1c5b413405bb2caf4a6a46501575f9e9d"
RETRY_PROPOSAL_AUTHORITY_SET_SHA256 = (
    "52f8246d8192f4fbf40c3aa415aee56bdbb5eb937f4778daa30fda42f06ad3a2"
)
RETRY_PROJECT_SCRIPT_EMBEDDED_REPOSITORY_CODE_SHA = "676d6b1429ee1ef60fbfc4de1d62f9d6ee9184ce"
RETRY_RESULT_ARTIFACT_SHA256 = "3e3b41b529294ac31c9559a6d46a7c8ad777063304adde72a72437d240751a09"

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


class DailyCapabilityRetrySessionEvidence(_StrictModel):
    session_date: date
    option_chain_present: Literal[True]
    contract_count: int
    two_sided_quote_count: int
    positive_open_interest_count: int
    finite_greeks_count: int
    finite_implied_volatility_count: int
    raw_rows_logged: Literal[False]
    orders_submitted: Literal[0]

    @model_validator(mode="after")
    def _validate_session(self) -> Self:
        if self.session_date not in EXPECTED_SESSIONS:
            raise ValueError("session date is outside the frozen expected sessions")
        if self.contract_count <= 0:
            raise ValueError("contract_count must be positive")
        for field in (
            "two_sided_quote_count",
            "finite_greeks_count",
            "finite_implied_volatility_count",
        ):
            if getattr(self, field) != self.contract_count:
                raise ValueError(f"{field} must cover every observed contract")
        if not 0 < self.positive_open_interest_count <= self.contract_count:
            raise ValueError("positive_open_interest_count must be bounded and positive")
        return self


class QCQQQOptionsDailyCapabilityGateRetryEvidence(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_capability_gate_retry_evidence.v1"]
    record_id: str
    collected_at_utc: datetime
    owner_authorization_id: str
    authorization_state: Literal["INVALIDATED_AFTER_EVIDENCE_COLLECTION"]
    proposal_repository_code_sha: Literal["c880bb9e55dbcf5c641756e80fdd2f9d00eaa0e2"]
    ordinary_pushed_main_sha: Literal["ab22067ab9f57cc11144ae4eef899cb21f639181"]
    proposal_file_sha256: Literal[
        "d5ecad8167e2abef7e5a8d6427604da5b6f59d4be50607228097191eba74239e"
    ]
    proposal_content_sha256: Literal[
        "77570e7ff88e1c567c29d10dcfc534cef07628cab58ceb894da79c6075f013b9"
    ]
    policy_file_sha256: Literal["851ee0fb3c2a14b25263b37115ece581869fee08dffac95e272960108c46bb19"]
    policy_canonical_sha256: Literal[
        "540107c9dce0fa08a8f461f8c733a1c1c5b413405bb2caf4a6a46501575f9e9d"
    ]
    proposal_authority_set_sha256: Literal[
        "52f8246d8192f4fbf40c3aa415aee56bdbb5eb937f4778daa30fda42f06ad3a2"
    ]
    account_tier: Literal["FREE"]
    cloud_compute_ui_label: Literal["Free Node"]
    account_verification_precheck: Literal["PASS_NO_VERIFICATION_GATE_OBSERVED"]
    project_id: Literal[34808569]
    project_name: Literal["Sleepy Yellow-Green Shark"]
    project_mutation_count: Literal[0]
    project_code_lf_sha256: Literal[
        "1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139"
    ]
    project_code_lf_byte_count: Literal[6148]
    project_script_embedded_repository_code_sha: Literal["676d6b1429ee1ef60fbfc4de1d62f9d6ee9184ce"]
    cloud_backtest_count: Literal[1]
    second_cloud_backtest_used: Literal[False]
    build_id: Literal["cd73fe-0a3a57"]
    engine_version: Literal["LEAN Engine v2.5.0.0.17989"]
    backtest_id: Literal["077252aa78ce2e0a7c3b9b4c38a554f7"]
    backtest_name: Literal["Jumping Blue Pig"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    evaluated_sessions: tuple[date, ...]
    session_evidence: tuple[DailyCapabilityRetrySessionEvidence, ...]
    deployment_seconds: Literal["16.13"]
    runtime_seconds: Literal["14.45"]
    data_points_per_second_display: Literal["4k"]
    processed_data_points: Literal[63982]
    result_state: Literal["Completed"]
    result_started_at_utc: datetime
    result_completed_at_utc: datetime
    result_artifact_filename: Literal["Jumping Blue Pig.json"]
    result_artifact_byte_count: Literal[16776]
    result_artifact_sha256: Literal[
        "3e3b41b529294ac31c9559a6d46a7c8ad777063304adde72a72437d240751a09"
    ]
    result_top_level_keys: tuple[str, ...]
    result_order_count: Literal[0]
    total_orders: Literal[0]
    fills: Literal[0]
    portfolio_invested: Literal[False]
    start_equity_usd: Literal["100000.00"]
    end_equity_usd: Literal["100000.00"]
    total_fees_usd: Literal["0.00"]
    holdings_value_usd: Literal["0.00"]
    volume: Literal[0]
    raw_options_rows_present: Literal[False]
    raw_rows_logged: Literal[False]
    prohibited_actions_observed: Literal[False]
    investment_interpretation_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    candidate_gate_status: Literal["GO_FOR_DAILY_ENGINEERING_ONLY"]
    decision: Literal["DAILY_CAPABILITY_EVIDENCE_COLLECTED_REVIEW_PENDING"]
    independent_review_status: Literal["PENDING_PROJECT_OWNER_REVIEW"]
    successor_registration_authorized: Literal[False]

    @field_validator("record_id")
    @classmethod
    def _validate_record_id(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator("collected_at_utc", "result_started_at_utc", "result_completed_at_utc")
    @classmethod
    def _validate_timestamp(cls, value: datetime, info: Any) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator(
        "proposal_repository_code_sha",
        "ordinary_pushed_main_sha",
        "project_script_embedded_repository_code_sha",
    )
    @classmethod
    def _validate_git_hash(cls, value: str, info: Any) -> str:
        return _git_sha(value, str(info.field_name))

    @field_validator(
        "proposal_file_sha256",
        "proposal_content_sha256",
        "policy_file_sha256",
        "policy_canonical_sha256",
        "proposal_authority_set_sha256",
        "project_code_lf_sha256",
        "result_artifact_sha256",
    )
    @classmethod
    def _validate_evidence_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        if self.owner_authorization_id != RETRY_OWNER_AUTHORIZATION_ID:
            raise ValueError("owner authorization identity drifted")
        if self.project_code_lf_sha256 != PREDECESSOR_SCRIPT_LF_SHA256:
            raise ValueError("project code identity drifted")
        if (
            self.requested_start != EXPECTED_SESSIONS[0]
            or self.evaluated_start != (EXPECTED_SESSIONS[0])
        ):
            raise ValueError("requested/evaluated start drifted")
        if (
            self.requested_end != EXPECTED_SESSIONS[-1]
            or self.evaluated_end != (EXPECTED_SESSIONS[-1])
        ):
            raise ValueError("requested/evaluated end drifted")
        if self.evaluated_sessions != EXPECTED_SESSIONS:
            raise ValueError("evaluated session inventory drifted")
        if tuple(item.session_date for item in self.session_evidence) != EXPECTED_SESSIONS:
            raise ValueError("session evidence inventory drifted")
        if self.result_top_level_keys != EXPECTED_RETRY_RESULT_TOP_LEVEL_KEYS:
            raise ValueError("result top-level key inventory drifted")
        if self.result_completed_at_utc < self.result_started_at_utc:
            raise ValueError("result completion precedes result start")
        return self


@dataclass(frozen=True)
class QCQQQOptionsDailyCapabilityGateRetryEvidenceLoadResult:
    evidence: QCQQQOptionsDailyCapabilityGateRetryEvidence
    evidence_path: Path
    evidence_file_sha256: str
    proposal: QCQQQOptionsDailyCapabilityGateRetryProposalLoadResult


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


def load_qc_qqq_options_daily_capability_gate_retry_evidence(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_PATH,
    *,
    proposal_path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_PROPOSAL_PATH,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateRetryEvidenceLoadResult:
    root = project_root.resolve()
    try:
        evidence_path = _require_bound_regular_file(
            path, project_root=root, field="daily capability retry evidence"
        )
        raw = evidence_path.read_bytes()
        evidence = QCQQQOptionsDailyCapabilityGateRetryEvidence.from_json_bytes(raw)
        proposal = load_qc_qqq_options_daily_capability_gate_retry_proposal(
            proposal_path,
            policy_path=policy_path,
            project_root=root,
        )
        if evidence.proposal_repository_code_sha != proposal.proposal.repository_code_sha:
            raise ValueError("proposal repository code SHA mismatch")
        if evidence.proposal_file_sha256 != proposal.proposal_file_sha256:
            raise ValueError("proposal file SHA-256 mismatch")
        if evidence.proposal_content_sha256 != proposal.proposal.content_sha256:
            raise ValueError("proposal content SHA-256 mismatch")
        if evidence.policy_file_sha256 != proposal.policy.policy_file_sha256:
            raise ValueError("policy file SHA-256 mismatch")
        if evidence.policy_canonical_sha256 != proposal.policy.policy_canonical_sha256:
            raise ValueError("policy canonical SHA-256 mismatch")
        if evidence.proposal_authority_set_sha256 != proposal.proposal.authority_set_sha256:
            raise ValueError("proposal authority-set SHA-256 mismatch")
    except QCQQQOptionsDailyCapabilityGateRetryContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateRetryContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsDailyCapabilityGateRetryEvidenceLoadResult(
        evidence=evidence,
        evidence_path=evidence_path,
        evidence_file_sha256=hashlib.sha256(raw).hexdigest(),
        proposal=proposal,
    )
