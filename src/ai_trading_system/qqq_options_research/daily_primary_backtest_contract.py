from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQReportRecord,
)
from ai_trading_system.qqq_options_research.daily_capability_gate_retry_review import (
    QCQQQOptionsDailyCapabilityGateRetryReviewLoadResult,
    load_qc_qqq_options_daily_capability_gate_retry_review,
)
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH = Path(
    "config/research/qqq_options_daily_primary_backtest_contract_v1.yaml"
)

_PRIMARY_START = date(2021, 2, 22)
_LEGACY_NON_DEFAULT_START = date(2022, 12, 1)
_UNKNOWN = "UNKNOWN_REQUIRES_POLICY_REVIEW"
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_SHARED_POLICY_SHA256 = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_DQ_PIT_POLICY_SHA256 = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_SIGNAL_EXPORT_POLICY_SHA256 = "cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9"
_ADAPTER_POLICY_SHA256 = "b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616"
_SELECTION_POLICY_SHA256 = "bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30"
_EXECUTION_POLICY_SHA256 = "8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a"
_ACCOUNTING_POLICY_SHA256 = "faa2659ee141cb2209686c3eadee31059ee660c3cc6d6dd3e63e259f23b1484e"
_LIFECYCLE_POLICY_SHA256 = "1798b6696e0f31571f9242a4276a06530fb951d15f250a2ef6756ac547037582"
_CAPABILITY_REVIEW_MODULE_SHA256 = (
    "ec643747d7c8f5a579dfec259e7b52bf75f026e5d30a26176cefd0ad599a34fb"
)
_CAPABILITY_REVIEW_FILE_SHA256 = "2c5ed5b80a101e0fc8a0285fabb941722189f3d034837df560292b1a031d132a"
_CAPABILITY_REVIEW_CONTENT_SHA256 = (
    "46690e117b7e89367bd37dcf1b17c28d6b097a7426a2bc3666337a52a621aded"
)
_CAPABILITY_EVIDENCE_FILE_SHA256 = (
    "829cd5de1d7691d98bfbf3554d27fabcda64598f3e26ce4747beddaf03f1c3b0"
)
_CAPABILITY_EVIDENCE_CONTENT_SHA256 = (
    "c19c2601e35fe6ee0495a041c1ddeafc52aa275a18856585b36ba2e6435fc609"
)
_CAPABILITY_RESULT_ARTIFACT_SHA256 = (
    "3e3b41b529294ac31c9559a6d46a7c8ad777063304adde72a72437d240751a09"
)
_DAILY_POLICY_SHA256 = "4a060600ef9d532e75449a09628a54b84c9b68eca41989e1e4ed18de54b3109a"
_DQ_SCOPE = "qqq_options_event_dq_pit_identity"
_DQ_POLICY_ID = "qqq_options_dq_pit_identity_v1"
_REQUIRED_DQ_CHECK_IDS = (
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "evidence_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "order_fill_chronology",
    "prior_day_model_freshness",
    "provider_raw_checksum",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
)
_AUTHORITY_PATHS = {
    "shared_policy_sha256": Path("config/research/qqq_options_shared_contract_v1.yaml"),
    "dq_pit_policy_sha256": Path("config/research/qqq_options_dq_pit_identity_v1.yaml"),
    "signal_export_policy_sha256": Path("config/research/qqq_options_signal_export_v1.yaml"),
    "adapter_policy_sha256": Path(
        "config/research/qc_qqq_options_project_adapter_contract_v1.yaml"
    ),
    "selection_policy_sha256": Path("config/research/qqq_options_deterministic_selection_v1.yaml"),
    "execution_policy_sha256": Path("config/research/qqq_options_minute_execution_reality_v1.yaml"),
    "accounting_policy_sha256": Path(
        "config/research/qqq_options_cash_premium_settlement_accounting_v1.yaml"
    ),
    "lifecycle_policy_sha256": Path(
        "config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml"
    ),
    "capability_review_module_sha256": Path(
        "src/ai_trading_system/qqq_options_research/daily_capability_gate_retry_review.py"
    ),
}

ResearchWindowRole = Literal["PRIMARY", "SENSITIVITY", "PROXY", "STRESS"]


class QQQOptionsDailyPrimaryBacktestContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field_name: str) -> str:
    if not value or value != value.strip() or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} must be non-empty normalized text")
    return value


def _identifier(value: str, field_name: str) -> str:
    checked = _required_text(value, field_name)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field_name} must be a portable identifier")
    return checked


def _sha256(value: str, field_name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return value


def _git_sha(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("repository_code_sha must be a lowercase Git SHA")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must use UTC offset")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _require_bound_regular_file(path: Path, *, project_root: Path, field: str) -> Path:
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
    return resolved


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def content_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.content_payload()))

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match canonical semantics")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QQQOptionsDailyPrimaryBacktestContractError(
                "QQQ_OPTIONS_DAILY_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_construct(**payload, content_sha256=_UNSEALED_SHA256)
        return cls(**payload, content_sha256=provisional.compute_content_sha256())

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _content_sha256(self.canonical_bytes)

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionsDailyPrimaryBacktestContractError(
                "QQQ_OPTIONS_DAILY_RECORD_INVALID", str(exc)
            ) from exc
        if content != record.canonical_bytes:
            raise QQQOptionsDailyPrimaryBacktestContractError(
                "QQQ_OPTIONS_DAILY_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical JSON encoding",
            )
        return record


class DailyPrimaryMarketDataPolicy(_PolicyModel):
    ticker: Literal["QQQ"]
    underlying_resolution: Literal["DAILY"]
    option_resolution: Literal["DAILY"]
    signal_resolution: Literal["DAILY"]
    normalization: Literal["RAW"]
    exchange_calendar: Literal["XNYS"]
    storage_timezone: Literal["UTC"]
    exchange_timezone: Literal["America/New_York"]


class DailyPrimaryDQAdmissionPolicy(_PolicyModel):
    scope: Literal["qqq_options_event_dq_pit_identity"]
    policy_id: Literal["qqq_options_dq_pit_identity_v1"]
    policy_version: Literal["1.0.0"]
    report_version: Literal["1.0.0"]
    required_check_ids: tuple[str, ...]

    @model_validator(mode="after")
    def _validate_checks(self) -> Self:
        if self.required_check_ids != _REQUIRED_DQ_CHECK_IDS:
            raise ValueError("required DQ checks differ from the reviewed 2482 freeze")
        return self


class DailyPrimaryBacktestChronologyPolicy(_PolicyModel):
    policy_id: Literal["qqq_options_daily_primary_chronology_v1"]
    signal_to_selection: Literal["PRIOR_COMPLETED_XNYS_SESSION"]
    selection_to_intent: Literal["STRICTLY_LATER_XNYS_SESSION"]
    intent_to_submit: Literal["SAME_OR_LATER_XNYS_SESSION"]
    submit_to_fill: Literal["STRICTLY_LATER_XNYS_SESSION"]
    fill_to_valuation: Literal["SAME_OR_LATER_XNYS_SESSION"]
    option_model_inputs: Literal["PRIOR_COMPLETED_XNYS_SESSION_ONLY"]
    daily_close_fill_allowed: Literal[False]
    same_bar_fill_allowed: Literal[False]
    lookahead_allowed: Literal[False]


class UnresolvedDailyPrimaryBacktestCriteria(_PolicyModel):
    mode: Literal["UNRESOLVED"]
    dte: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    moneyness: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    delta: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    spread: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    open_interest: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    volume: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    quote_freshness: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    fees: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    slippage: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    latency: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    partial_fill: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    cancellation: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    expiry: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    position_sizing: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    initial_cash: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    acceptance_threshold: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]


class QQQOptionsDailyPrimaryBacktestSafety(_PolicyModel):
    research_only: Literal[True]
    selection_allowed: Literal[False]
    order_intent_allowed: Literal[False]
    order_submit_allowed: Literal[False]
    fill_allowed: Literal[False]
    daily_close_fill_allowed: Literal[False]
    same_bar_fill_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    external_action_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsDailyPrimaryBacktestPolicy(_PolicyModel):
    schema_version: Literal["qqq_options_daily_primary_backtest_contract_policy.v1"]
    policy_id: Literal["qqq_options_daily_primary_backtest_contract_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED_BASELINE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    daily_engineering_authorized: Literal[True]
    backtest_execution_authorized: Literal[False]
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    signal_export_policy_sha256: str
    adapter_policy_sha256: str
    selection_policy_sha256: str
    execution_policy_sha256: str
    accounting_policy_sha256: str
    lifecycle_policy_sha256: str
    capability_review_module_sha256: str
    capability_review_file_sha256: str
    capability_review_content_sha256: str
    capability_evidence_file_sha256: str
    capability_evidence_content_sha256: str
    capability_result_artifact_sha256: str
    primary_research_start: date
    approved_non_primary_authorities: tuple[str, ...]
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    market_data: DailyPrimaryMarketDataPolicy
    dq_admission: DailyPrimaryDQAdmissionPolicy
    chronology: DailyPrimaryBacktestChronologyPolicy
    criteria: UnresolvedDailyPrimaryBacktestCriteria
    safety: QQQOptionsDailyPrimaryBacktestSafety

    @field_validator(
        "owner",
        "owner_decision",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "signal_export_policy_sha256",
        "adapter_policy_sha256",
        "selection_policy_sha256",
        "execution_policy_sha256",
        "accounting_policy_sha256",
        "lifecycle_policy_sha256",
        "capability_review_module_sha256",
        "capability_review_file_sha256",
        "capability_review_content_sha256",
        "capability_evidence_file_sha256",
        "capability_evidence_content_sha256",
        "capability_result_artifact_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        expected = {
            "shared_contract_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
            "shared_policy_sha256": _SHARED_POLICY_SHA256,
            "dq_pit_policy_sha256": _DQ_PIT_POLICY_SHA256,
            "signal_export_policy_sha256": _SIGNAL_EXPORT_POLICY_SHA256,
            "adapter_policy_sha256": _ADAPTER_POLICY_SHA256,
            "selection_policy_sha256": _SELECTION_POLICY_SHA256,
            "execution_policy_sha256": _EXECUTION_POLICY_SHA256,
            "accounting_policy_sha256": _ACCOUNTING_POLICY_SHA256,
            "lifecycle_policy_sha256": _LIFECYCLE_POLICY_SHA256,
            "capability_review_module_sha256": _CAPABILITY_REVIEW_MODULE_SHA256,
            "capability_review_file_sha256": _CAPABILITY_REVIEW_FILE_SHA256,
            "capability_review_content_sha256": _CAPABILITY_REVIEW_CONTENT_SHA256,
            "capability_evidence_file_sha256": _CAPABILITY_EVIDENCE_FILE_SHA256,
            "capability_evidence_content_sha256": _CAPABILITY_EVIDENCE_CONTENT_SHA256,
            "capability_result_artifact_sha256": _CAPABILITY_RESULT_ARTIFACT_SHA256,
        }
        for field_name, expected_value in expected.items():
            if getattr(self, field_name) != expected_value:
                raise ValueError(f"{field_name} differs from inherited exact authority")
        if self.primary_research_start != _PRIMARY_START:
            raise ValueError("primary research start must remain 2021-02-22")
        if self.approved_non_primary_authorities:
            raise ValueError("no non-primary research window authority is approved")
        if self.legacy_non_default_start != _LEGACY_NON_DEFAULT_START:
            raise ValueError("legacy non-default start marker drifted")
        return self


@dataclass(frozen=True)
class QQQOptionsDailyPrimaryBacktestPolicyLoadResult:
    policy: QQQOptionsDailyPrimaryBacktestPolicy
    policy_path: Path
    policy_sha256: str
    capability_review: QCQQQOptionsDailyCapabilityGateRetryReviewLoadResult


class QQQOptionsDailyPrimaryBacktestRequest(_StrictModel):
    run_id: str
    created_at_utc: datetime
    repository_code_sha: str
    research_window_role: ResearchWindowRole
    reviewed_role_authority_id: str | None
    dq_caveat: str | None
    ticker: Literal["QQQ"]
    underlying_resolution: Literal["DAILY"]
    option_resolution: Literal["DAILY"]
    signal_resolution: Literal["DAILY"]
    normalization: Literal["RAW"]
    exchange_calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    evaluated_sessions: tuple[date, ...]
    source_id: str
    source_checksum: str
    dq_report_bytes: bytes
    dq_report_file_sha256: str

    @field_validator("run_id", "source_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("reviewed_role_authority_id")
    @classmethod
    def _validate_optional_id(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "reviewed_role_authority_id")

    @field_validator("dq_caveat")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "dq_caveat")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_sha(value)

    @field_validator("source_checksum", "dq_report_file_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_request(self) -> Self:
        if self.requested_end < self.requested_start:
            raise ValueError("requested range is reversed")
        if self.evaluated_end < self.evaluated_start:
            raise ValueError("evaluated range is reversed")
        if self.evaluated_start < self.requested_start or self.evaluated_end > self.requested_end:
            raise ValueError("evaluated range must be contained in requested range")
        if not self.evaluated_sessions or len(set(self.evaluated_sessions)) != len(
            self.evaluated_sessions
        ):
            raise ValueError("evaluated sessions must be non-empty and unique")
        if _content_sha256(self.dq_report_bytes) != self.dq_report_file_sha256:
            raise ValueError("DQ report file hash differs from supplied bytes")
        DQReportRecord.from_json_bytes(self.dq_report_bytes)
        return self


class CanonicalDailyDQAdmission(_StrictModel):
    report_file_sha256: str
    report_content_sha256: str
    record_id: str
    lineage_id: str
    scope: Literal["qqq_options_event_dq_pit_identity"]
    report_version: Literal["1.0.0"]
    generated_at_utc: datetime
    dq_status: Literal["PASS"]
    pit_status: Literal["PASS"]
    source_id: str
    source_checksum: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    passed_check_ids: tuple[str, ...]
    derivation: Literal["CANONICAL_DQ_REPORT_FACTS"]

    @field_validator("report_file_sha256", "report_content_sha256", "source_checksum")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("record_id", "lineage_id", "source_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("generated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "generated_at_utc")

    @model_validator(mode="after")
    def _validate_checks(self) -> Self:
        if self.passed_check_ids != _REQUIRED_DQ_CHECK_IDS:
            raise ValueError("DQ admission must contain all reviewed checks")
        return self


class QQQOptionsDailyPrimaryBacktestDescriptor(_SealedModel):
    schema_version: Literal["qqq_options_daily_primary_backtest_descriptor.v1"]
    run_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_id: Literal["qqq_options_daily_primary_backtest_contract_v1"]
    policy_version: Literal["1.0.0"]
    policy_sha256: str
    capability_review_file_sha256: str
    capability_review_content_sha256: str
    capability_evidence_file_sha256: str
    capability_evidence_content_sha256: str
    capability_result_artifact_sha256: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    signal_export_policy_sha256: str
    adapter_policy_sha256: str
    selection_policy_sha256: str
    execution_policy_sha256: str
    accounting_policy_sha256: str
    lifecycle_policy_sha256: str
    research_window_role: Literal["PRIMARY"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    evaluated_sessions: tuple[date, ...]
    market_data: DailyPrimaryMarketDataPolicy
    chronology: DailyPrimaryBacktestChronologyPolicy
    dq_admission: CanonicalDailyDQAdmission
    source_identity_sha256: str
    input_admission_status: Literal["PASS_DQ_CONTRACT_ONLY"]
    selection_status: Literal["OWNER_REVIEW_REQUIRED"]
    execution_status: Literal["OWNER_REVIEW_REQUIRED"]
    accounting_status: Literal["OWNER_REVIEW_REQUIRED"]
    lifecycle_status: Literal["OWNER_REVIEW_REQUIRED"]
    fee_identity: Literal["NOT_EVALUATED_POLICY_BLOCKED"]
    slippage_identity: Literal["NOT_EVALUATED_POLICY_BLOCKED"]
    fill_identity: Literal["NOT_EVALUATED_POLICY_BLOCKED"]
    order_count: Literal[0]
    fill_count: Literal[0]
    disposition: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    reason_codes: tuple[
        Literal[
            "ACCOUNTING_POLICY_REVIEW_REQUIRED",
            "EXECUTION_POLICY_REVIEW_REQUIRED",
            "LIFECYCLE_POLICY_REVIEW_REQUIRED",
            "SELECTION_POLICY_REVIEW_REQUIRED",
        ],
        ...,
    ]
    safety: QQQOptionsDailyPrimaryBacktestSafety

    @field_validator(
        "policy_sha256",
        "capability_review_file_sha256",
        "capability_review_content_sha256",
        "capability_evidence_file_sha256",
        "capability_evidence_content_sha256",
        "capability_result_artifact_sha256",
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "signal_export_policy_sha256",
        "adapter_policy_sha256",
        "selection_policy_sha256",
        "execution_policy_sha256",
        "accounting_policy_sha256",
        "lifecycle_policy_sha256",
        "source_identity_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("run_id")
    @classmethod
    def _validate_run_id(cls, value: str) -> str:
        return _identifier(value, "run_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value)

    @model_validator(mode="after")
    def _validate_descriptor(self) -> Self:
        expected_hashes = {
            "policy_sha256": _DAILY_POLICY_SHA256,
            "capability_review_file_sha256": _CAPABILITY_REVIEW_FILE_SHA256,
            "capability_review_content_sha256": _CAPABILITY_REVIEW_CONTENT_SHA256,
            "capability_evidence_file_sha256": _CAPABILITY_EVIDENCE_FILE_SHA256,
            "capability_evidence_content_sha256": _CAPABILITY_EVIDENCE_CONTENT_SHA256,
            "capability_result_artifact_sha256": _CAPABILITY_RESULT_ARTIFACT_SHA256,
            "shared_contract_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
            "shared_policy_sha256": _SHARED_POLICY_SHA256,
            "dq_pit_policy_sha256": _DQ_PIT_POLICY_SHA256,
            "signal_export_policy_sha256": _SIGNAL_EXPORT_POLICY_SHA256,
            "adapter_policy_sha256": _ADAPTER_POLICY_SHA256,
            "selection_policy_sha256": _SELECTION_POLICY_SHA256,
            "execution_policy_sha256": _EXECUTION_POLICY_SHA256,
            "accounting_policy_sha256": _ACCOUNTING_POLICY_SHA256,
            "lifecycle_policy_sha256": _LIFECYCLE_POLICY_SHA256,
        }
        for field_name, expected in expected_hashes.items():
            if getattr(self, field_name) != expected:
                raise ValueError(f"{field_name} differs from exact inherited authority")
        if self.requested_start != _PRIMARY_START or self.evaluated_start != _PRIMARY_START:
            raise ValueError("PRIMARY descriptor must start on 2021-02-22")
        if self.requested_end < self.requested_start:
            raise ValueError("requested range is reversed")
        if self.evaluated_end < self.evaluated_start:
            raise ValueError("evaluated range is reversed")
        if self.evaluated_start < self.requested_start or self.evaluated_end > self.requested_end:
            raise ValueError("evaluated range must be contained in requested range")
        if self.evaluated_sessions != tuple(sorted(self.evaluated_sessions)):
            raise ValueError("descriptor session inventory must be sorted")
        if self.evaluated_sessions != _trading_sessions(self.evaluated_start, self.evaluated_end):
            raise ValueError("descriptor session inventory differs from XNYS calendar")
        if (
            self.dq_admission.requested_start != self.requested_start
            or self.dq_admission.requested_end != self.requested_end
            or self.dq_admission.evaluated_start != self.evaluated_start
            or self.dq_admission.evaluated_end != self.evaluated_end
            or self.dq_admission.generated_at_utc > self.created_at_utc
        ):
            raise ValueError("DQ admission range or as-of cross-binding mismatch")
        expected_source_identity = _content_sha256(
            _canonical_json_bytes(
                {
                    "dq_report_file_sha256": self.dq_admission.report_file_sha256,
                    "repository_code_sha": self.repository_code_sha,
                    "sessions": [item.isoformat() for item in self.evaluated_sessions],
                    "source_checksum": self.dq_admission.source_checksum,
                    "source_id": self.dq_admission.source_id,
                }
            )
        )
        if self.source_identity_sha256 != expected_source_identity:
            raise ValueError("source identity differs from canonical bound inputs")
        expected_reasons = (
            "ACCOUNTING_POLICY_REVIEW_REQUIRED",
            "EXECUTION_POLICY_REVIEW_REQUIRED",
            "LIFECYCLE_POLICY_REVIEW_REQUIRED",
            "SELECTION_POLICY_REVIEW_REQUIRED",
        )
        if self.reason_codes != expected_reasons:
            raise ValueError("blocked reason taxonomy drifted")
        return self


def _trading_sessions(start: date, end: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while current <= end:
        if us_equity_market_session(current).is_trading_day:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _derive_dq_admission(
    request: QQQOptionsDailyPrimaryBacktestRequest,
) -> CanonicalDailyDQAdmission:
    report = DQReportRecord.from_json_bytes(request.dq_report_bytes)
    if report.scope != _DQ_SCOPE or report.report_version != "1.0.0":
        raise ValueError("DQ report scope or version mismatch")
    if (
        report.policy_id != _DQ_POLICY_ID
        or report.policy_version != "1.0.0"
        or report.policy_sha256 != _DQ_PIT_POLICY_SHA256
        or report.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256
    ):
        raise ValueError("DQ report policy or contract identity mismatch")
    if report.repository_code_sha != request.repository_code_sha:
        raise ValueError("DQ report repository code SHA mismatch")
    if (
        report.generated_at_utc > request.created_at_utc
        or report.created_at_utc > request.created_at_utc
    ):
        raise ValueError("DQ report as-of time exceeds descriptor creation time")
    report_range = (
        report.requested_start,
        report.requested_end,
        report.evaluated_start,
        report.evaluated_end,
    )
    request_range = (
        request.requested_start,
        request.requested_end,
        request.evaluated_start,
        request.evaluated_end,
    )
    if report_range != request_range:
        raise ValueError("DQ report requested/evaluated range mismatch")
    if report.storage_timezone != "UTC" or report.exchange_timezone != "America/New_York":
        raise ValueError("DQ report timezone identity mismatch")
    if report.dq_status != "PASS" or report.pit_status != "PASS":
        raise ValueError("DQ and PIT status must both derive as PASS")
    check_ids = tuple(item.check_id for item in report.checks)
    if check_ids != _REQUIRED_DQ_CHECK_IDS or any(item.status != "PASS" for item in report.checks):
        raise ValueError("DQ report does not contain the exact all-PASS reviewed checks")
    source_pairs = tuple(zip(report.source_ids, report.source_checksums, strict=True))
    if (request.source_id, request.source_checksum) not in source_pairs:
        raise ValueError("DQ report source identity or checksum mismatch")
    return CanonicalDailyDQAdmission(
        report_file_sha256=request.dq_report_file_sha256,
        report_content_sha256=report.content_sha256,
        record_id=report.record_id,
        lineage_id=report.lineage_id,
        scope="qqq_options_event_dq_pit_identity",
        report_version="1.0.0",
        generated_at_utc=report.generated_at_utc,
        dq_status="PASS",
        pit_status="PASS",
        source_id=request.source_id,
        source_checksum=request.source_checksum,
        requested_start=request.requested_start,
        requested_end=request.requested_end,
        evaluated_start=request.evaluated_start,
        evaluated_end=request.evaluated_end,
        passed_check_ids=check_ids,
        derivation="CANONICAL_DQ_REPORT_FACTS",
    )


def load_qqq_options_daily_primary_backtest_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDailyPrimaryBacktestPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(path, project_root=root, field="daily policy")
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsDailyPrimaryBacktestPolicy.model_validate(payload, strict=False)
        for field_name, authority_path in _AUTHORITY_PATHS.items():
            resolved = _require_bound_regular_file(
                authority_path, project_root=root, field=f"{field_name} authority"
            )
            if _content_sha256(resolved.read_bytes()) != getattr(policy, field_name):
                raise ValueError(f"{field_name} tracked authority bytes drifted")
        review = load_qc_qqq_options_daily_capability_gate_retry_review(project_root=root)
        if review.review_file_sha256 != policy.capability_review_file_sha256:
            raise ValueError("capability review file hash drifted")
        if review.review.content_sha256 != policy.capability_review_content_sha256:
            raise ValueError("capability review content hash drifted")
        if review.review.accepted_candidate_gate_status != "GO_FOR_DAILY_ENGINEERING_ONLY":
            raise ValueError("capability review does not authorize DAILY engineering")
        if review.review.successor_scope != "DAILY_ENGINEERING_ONLY":
            raise ValueError("capability review successor scope mismatch")
    except QQQOptionsDailyPrimaryBacktestContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsDailyPrimaryBacktestContractError(
            "QQQ_OPTIONS_DAILY_POLICY_INVALID", str(exc)
        ) from exc
    return QQQOptionsDailyPrimaryBacktestPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=_content_sha256(raw),
        capability_review=review,
    )


def build_qqq_options_daily_primary_backtest_descriptor(
    request: QQQOptionsDailyPrimaryBacktestRequest,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDailyPrimaryBacktestDescriptor:
    try:
        loaded = load_qqq_options_daily_primary_backtest_policy(
            path=policy_path, project_root=project_root
        )
        policy = loaded.policy
        if request.research_window_role != "PRIMARY":
            if (
                request.reviewed_role_authority_id is None
                or request.dq_caveat is None
                or request.reviewed_role_authority_id not in policy.approved_non_primary_authorities
            ):
                raise ValueError("non-primary window lacks reviewed role authority and DQ caveat")
            raise ValueError("this v1 descriptor admits PRIMARY runs only")
        if request.reviewed_role_authority_id is not None or request.dq_caveat is not None:
            raise ValueError("PRIMARY requests cannot claim a non-primary role authority")
        if request.requested_start != _PRIMARY_START or request.evaluated_start != _PRIMARY_START:
            raise ValueError("PRIMARY requested/evaluated start must be 2021-02-22")
        expected_sessions = _trading_sessions(request.evaluated_start, request.evaluated_end)
        if tuple(sorted(request.evaluated_sessions)) != expected_sessions:
            raise ValueError("evaluated session inventory differs from reviewed XNYS calendar")
        dq_admission = _derive_dq_admission(request)
        source_identity_sha256 = _content_sha256(
            _canonical_json_bytes(
                {
                    "dq_report_file_sha256": request.dq_report_file_sha256,
                    "repository_code_sha": request.repository_code_sha,
                    "sessions": [item.isoformat() for item in expected_sessions],
                    "source_checksum": request.source_checksum,
                    "source_id": request.source_id,
                }
            )
        )
        return QQQOptionsDailyPrimaryBacktestDescriptor.seal(
            schema_version="qqq_options_daily_primary_backtest_descriptor.v1",
            run_id=request.run_id,
            created_at_utc=request.created_at_utc,
            repository_code_sha=request.repository_code_sha,
            policy_id=policy.policy_id,
            policy_version=policy.policy_version,
            policy_sha256=loaded.policy_sha256,
            capability_review_file_sha256=policy.capability_review_file_sha256,
            capability_review_content_sha256=policy.capability_review_content_sha256,
            capability_evidence_file_sha256=policy.capability_evidence_file_sha256,
            capability_evidence_content_sha256=policy.capability_evidence_content_sha256,
            capability_result_artifact_sha256=policy.capability_result_artifact_sha256,
            shared_contract_sha256=policy.shared_contract_sha256,
            shared_policy_sha256=policy.shared_policy_sha256,
            dq_pit_policy_sha256=policy.dq_pit_policy_sha256,
            signal_export_policy_sha256=policy.signal_export_policy_sha256,
            adapter_policy_sha256=policy.adapter_policy_sha256,
            selection_policy_sha256=policy.selection_policy_sha256,
            execution_policy_sha256=policy.execution_policy_sha256,
            accounting_policy_sha256=policy.accounting_policy_sha256,
            lifecycle_policy_sha256=policy.lifecycle_policy_sha256,
            research_window_role="PRIMARY",
            requested_start=request.requested_start,
            requested_end=request.requested_end,
            evaluated_start=request.evaluated_start,
            evaluated_end=request.evaluated_end,
            evaluated_sessions=expected_sessions,
            market_data=policy.market_data,
            chronology=policy.chronology,
            dq_admission=dq_admission,
            source_identity_sha256=source_identity_sha256,
            input_admission_status="PASS_DQ_CONTRACT_ONLY",
            selection_status="OWNER_REVIEW_REQUIRED",
            execution_status="OWNER_REVIEW_REQUIRED",
            accounting_status="OWNER_REVIEW_REQUIRED",
            lifecycle_status="OWNER_REVIEW_REQUIRED",
            fee_identity="NOT_EVALUATED_POLICY_BLOCKED",
            slippage_identity="NOT_EVALUATED_POLICY_BLOCKED",
            fill_identity="NOT_EVALUATED_POLICY_BLOCKED",
            order_count=0,
            fill_count=0,
            disposition="POLICY_BLOCKED_CASH_PRESERVATION",
            reason_codes=(
                "ACCOUNTING_POLICY_REVIEW_REQUIRED",
                "EXECUTION_POLICY_REVIEW_REQUIRED",
                "LIFECYCLE_POLICY_REVIEW_REQUIRED",
                "SELECTION_POLICY_REVIEW_REQUIRED",
            ),
            safety=policy.safety,
        )
    except QQQOptionsDailyPrimaryBacktestContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsDailyPrimaryBacktestContractError(
            "QQQ_OPTIONS_DAILY_DESCRIPTOR_INVALID", str(exc)
        ) from exc


def load_qqq_options_daily_primary_backtest_descriptor(
    path: Path,
    *,
    dq_report_path: Path,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDailyPrimaryBacktestDescriptor:
    try:
        resolved = _require_bound_regular_file(
            path, project_root=project_root, field="daily descriptor"
        )
        descriptor = QQQOptionsDailyPrimaryBacktestDescriptor.from_json_bytes(resolved.read_bytes())
        report_path = _require_bound_regular_file(
            dq_report_path,
            project_root=project_root,
            field="daily descriptor DQ report",
        )
        report_bytes = report_path.read_bytes()
        request = QQQOptionsDailyPrimaryBacktestRequest(
            run_id=descriptor.run_id,
            created_at_utc=descriptor.created_at_utc,
            repository_code_sha=descriptor.repository_code_sha,
            research_window_role="PRIMARY",
            reviewed_role_authority_id=None,
            dq_caveat=None,
            ticker=descriptor.market_data.ticker,
            underlying_resolution=descriptor.market_data.underlying_resolution,
            option_resolution=descriptor.market_data.option_resolution,
            signal_resolution=descriptor.market_data.signal_resolution,
            normalization=descriptor.market_data.normalization,
            exchange_calendar=descriptor.market_data.exchange_calendar,
            requested_start=descriptor.requested_start,
            requested_end=descriptor.requested_end,
            evaluated_start=descriptor.evaluated_start,
            evaluated_end=descriptor.evaluated_end,
            evaluated_sessions=descriptor.evaluated_sessions,
            source_id=descriptor.dq_admission.source_id,
            source_checksum=descriptor.dq_admission.source_checksum,
            dq_report_bytes=report_bytes,
            dq_report_file_sha256=_content_sha256(report_bytes),
        )
        if _derive_dq_admission(request) != descriptor.dq_admission:
            raise ValueError("descriptor DQ admission does not replay from report facts")
        return descriptor
    except QQQOptionsDailyPrimaryBacktestContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsDailyPrimaryBacktestContractError(
            "QQQ_OPTIONS_DAILY_DESCRIPTOR_INVALID", str(exc)
        ) from exc


__all__ = [
    "CanonicalDailyDQAdmission",
    "DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH",
    "DailyPrimaryBacktestChronologyPolicy",
    "QQQOptionsDailyPrimaryBacktestContractError",
    "QQQOptionsDailyPrimaryBacktestDescriptor",
    "QQQOptionsDailyPrimaryBacktestPolicy",
    "QQQOptionsDailyPrimaryBacktestPolicyLoadResult",
    "QQQOptionsDailyPrimaryBacktestRequest",
    "ResearchWindowRole",
    "build_qqq_options_daily_primary_backtest_descriptor",
    "load_qqq_options_daily_primary_backtest_descriptor",
    "load_qqq_options_daily_primary_backtest_policy",
]
