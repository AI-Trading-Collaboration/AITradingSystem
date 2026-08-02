from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.cash_accounting import (
    QQQOptionAccountingPosition,
    QQQOptionCashAccountingResult,
)
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    CanonicalDecimal,
    ContractCandidateSnapshotRecord,
    DQReportRecord,
    DQStatus,
    PortfolioSnapshotRecord,
    PositionLifecycleEventRecord,
    PositionLifecycleState,
    QQQOptionsSafetyBoundary,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.minute_execution import (
    QQQOptionExecutionResult,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH = Path(
    "config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml"
)

_UNKNOWN = "UNKNOWN_REQUIRES_POLICY_REVIEW"
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_EXCHANGE_TIMEZONE = ZoneInfo("America/New_York")
_SHARED_POLICY_SHA256 = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_DQ_PIT_POLICY_SHA256 = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_ADAPTER_POLICY_SHA256 = "b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616"
_TRACKED_SELECTION_POLICY_SHA256 = (
    "bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30"
)
_TRACKED_EXECUTION_POLICY_SHA256 = (
    "8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a"
)
_TRACKED_ACCOUNTING_POLICY_SHA256 = (
    "faa2659ee141cb2209686c3eadee31059ee660c3cc6d6dd3e63e259f23b1484e"
)

LifecycleScenarioRole = Literal[
    "REALITY_BASELINE",
    "SENSITIVITY",
    "SYNTHETIC_TEST_ONLY",
]
LifecycleObservationKind = Literal["EXIT_QUOTE", "EXPIRY_SETTLEMENT"]
LifecycleExternalEventType = Literal[
    "EXERCISE",
    "ASSIGNMENT",
    "UNDERLYING_SPLIT",
    "SPECIAL_DIVIDEND",
    "MERGER",
    "SYMBOL_CHANGE",
    "OPTION_CONTRACT_ADJUSTMENT",
]
LifecycleReason = Literal[
    "LIFECYCLE_POLICY_REVIEW_REQUIRED",
    "ACCOUNTING_REPLAY_BLOCKED_CASH_PRESERVED",
    "LIFECYCLE_INPUT_INVALID",
    "LIFECYCLE_REPLAY_READY",
    "PRE_EXPIRY_EXIT_REQUIRED",
    "PRE_EXPIRY_EXIT_BLOCKED",
    "EXPIRY_CLOSED_WORTHLESS",
    "EXPIRY_SCOPE_VIOLATION_INVALID_RUN",
    "EXTERNAL_SCOPE_VIOLATION_INVALID_RUN",
    "UNRESOLVED_EXPIRY_INVALID_RUN",
]


class QQQOptionPositionLifecycleContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _ReplayAbort(Exception):
    pass


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


def _utc(value: datetime, field_name: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must use UTC offset")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
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


def _content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _worst_status(statuses: list[DQStatus]) -> DQStatus:
    if "FAIL" in statuses:
        return "FAIL"
    if "NOT_EVALUATED" in statuses:
        return "NOT_EVALUATED"
    return "PASS"


def _shared_safety() -> QQQOptionsSafetyBoundary:
    return QQQOptionsSafetyBoundary(
        research_only=True,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        raw_options_data_export_allowed=False,
        strategy_execution_allowed=False,
        bounded_cloud_pilot_authorized=False,
        production_effect="none",
        broker_action="none",
    )


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
            raise QQQOptionPositionLifecycleContractError(
                "QQQ_OPTION_LIFECYCLE_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_construct(**payload, content_sha256=_UNSEALED_SHA256)
        return cls(**payload, content_sha256=provisional.compute_content_sha256())

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionPositionLifecycleContractError(
                "QQQ_OPTION_LIFECYCLE_RECORD_INVALID", str(exc)
            ) from exc
        if content != record.canonical_bytes:
            raise QQQOptionPositionLifecycleContractError(
                "QQQ_OPTION_LIFECYCLE_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical JSON encoding",
            )
        return record


class UnresolvedPositionLifecycleCriteria(_PolicyModel):
    mode: Literal["UNRESOLVED"]
    scenario_role: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    dq_caveat: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    pre_expiry_guard_sessions: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_exit_quote_age_ms: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    expiry_settlement_source_policy: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    scope_violation_disposition: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    reality_baseline: Literal[False]


class ActivePositionLifecycleCriteria(_PolicyModel):
    mode: Literal["ACTIVE"]
    scenario_role: LifecycleScenarioRole
    dq_caveat: str
    pre_expiry_guard_sessions: int = Field(ge=0)
    max_exit_quote_age_ms: int = Field(gt=0)
    expiry_settlement_source_policy: Literal["REVIEWED_EVENT_DQ_REPORT"]
    scope_violation_disposition: Literal["INVALIDATE_RUN_WITHOUT_DELIVERY"]
    reality_baseline: bool

    @field_validator("dq_caveat")
    @classmethod
    def _validate_caveat(cls, value: str) -> str:
        return _required_text(value, "dq_caveat")

    @model_validator(mode="after")
    def _validate_active(self) -> Self:
        if self.scenario_role == "REALITY_BASELINE" and not self.reality_baseline:
            raise ValueError("REALITY_BASELINE must declare reality_baseline=true")
        if self.scenario_role != "REALITY_BASELINE" and self.reality_baseline:
            raise ValueError("non-baseline scenario cannot claim a reality baseline")
        return self


PositionLifecycleCriteria = Annotated[
    UnresolvedPositionLifecycleCriteria | ActivePositionLifecycleCriteria,
    Field(discriminator="mode"),
]


class QQQOptionPositionLifecycleSafety(_PolicyModel):
    research_only: Literal[True]
    long_premium_only: Literal[True]
    short_option_allowed: Literal[False]
    multi_leg_allowed: Literal[False]
    roll_allowed: Literal[False]
    margin_allowed: Literal[False]
    underlying_share_delivery_allowed: Literal[False]
    short_underlying_allowed: Literal[False]
    assignment_allowed: Literal[False]
    exercise_completion_allowed: Literal[False]
    corporate_action_adjustment_allowed: Literal[False]
    new_order_intent_allowed: Literal[False]
    new_fill_allowed: Literal[False]
    daily_close_exit_allowed: Literal[False]
    same_bar_fill_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    external_order_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionPositionLifecyclePolicy(_PolicyModel):
    schema_version: Literal[
        "qqq_options_lifecycle_expiry_corporate_action_safety_policy.v1"
    ]
    policy_id: Literal["qqq_options_lifecycle_expiry_corporate_action_safety_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED_BASELINE", "OWNER_REVIEWED_ACTIVE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    lifecycle_authorized: bool
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    adapter_policy_sha256: str
    selection_policy_sha256: str
    execution_policy_sha256: str
    accounting_policy_sha256: str
    primary_research_start: date
    approved_non_primary_authority_count: int
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    criteria: PositionLifecycleCriteria
    safety: QQQOptionPositionLifecycleSafety

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
        "adapter_policy_sha256",
        "selection_policy_sha256",
        "execution_policy_sha256",
        "accounting_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("lifecycle policy must bind the exact shared contract")
        if self.shared_policy_sha256 != _SHARED_POLICY_SHA256:
            raise ValueError("lifecycle policy must bind the exact shared policy")
        if self.dq_pit_policy_sha256 != _DQ_PIT_POLICY_SHA256:
            raise ValueError("lifecycle policy must bind the exact DQ/PIT policy")
        if self.adapter_policy_sha256 != _ADAPTER_POLICY_SHA256:
            raise ValueError("lifecycle policy must bind the exact adapter policy")
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.approved_non_primary_authority_count != 0:
            raise ValueError("no non-primary research window is approved")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        active = isinstance(self.criteria, ActivePositionLifecycleCriteria)
        if self.status == "OWNER_REVIEWED_ACTIVE":
            if not self.lifecycle_authorized or not active:
                raise ValueError("active lifecycle policy requires active criteria")
        elif self.lifecycle_authorized or active:
            raise ValueError("baseline lifecycle policy must remain unauthorized")
        if self.status == "OWNER_REVIEW_REQUIRED_BASELINE" and (
            self.selection_policy_sha256 != _TRACKED_SELECTION_POLICY_SHA256
            or self.execution_policy_sha256 != _TRACKED_EXECUTION_POLICY_SHA256
            or self.accounting_policy_sha256 != _TRACKED_ACCOUNTING_POLICY_SHA256
        ):
            raise ValueError("baseline must bind the exact blocked predecessor policies")
        return self


@dataclass(frozen=True)
class QQQOptionPositionLifecyclePolicyLoadResult:
    policy: QQQOptionPositionLifecyclePolicy
    policy_path: Path
    policy_sha256: str


class QQQOptionExecutionResultArtifact(_StrictModel):
    content: bytes
    file_sha256: str

    @field_validator("file_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")

    @model_validator(mode="after")
    def _validate_artifact(self) -> Self:
        if _content_sha256(self.content) != self.file_sha256:
            raise ValueError("execution result file hash differs from bytes")
        QQQOptionExecutionResult.from_json_bytes(self.content)
        return self

    @property
    def result(self) -> QQQOptionExecutionResult:
        return QQQOptionExecutionResult.from_json_bytes(self.content)


class QQQOptionCandidateSnapshotArtifact(_StrictModel):
    content: bytes
    file_sha256: str

    @field_validator("file_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")

    @model_validator(mode="after")
    def _validate_artifact(self) -> Self:
        if _content_sha256(self.content) != self.file_sha256:
            raise ValueError("candidate snapshot file hash differs from bytes")
        ContractCandidateSnapshotRecord.from_json_bytes(self.content)
        return self

    @property
    def candidate(self) -> ContractCandidateSnapshotRecord:
        return ContractCandidateSnapshotRecord.from_json_bytes(self.content)


class _DQBoundInput(_StrictModel):
    source_id: str
    source_sha256: str
    dq_report_bytes: bytes
    dq_report_file_sha256: str

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        return _identifier(value, "source_id")

    @field_validator("source_sha256", "dq_report_file_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_report_bytes(self) -> Self:
        if _content_sha256(self.dq_report_bytes) != self.dq_report_file_sha256:
            raise ValueError("DQ report file hash differs from bytes")
        DQReportRecord.from_json_bytes(self.dq_report_bytes)
        return self

    @property
    def dq_report(self) -> DQReportRecord:
        return DQReportRecord.from_json_bytes(self.dq_report_bytes)


class QQQOptionLifecycleMarketObservation(_DQBoundInput):
    observation_id: str
    observation_kind: LifecycleObservationKind
    option_sid: str
    observed_at_utc: datetime
    effective_session: date
    bid_per_share: CanonicalDecimal | None
    ask_per_share: CanonicalDecimal | None
    underlying_price_usd_per_share: CanonicalDecimal | None

    @field_validator("observation_id")
    @classmethod
    def _validate_observation_id(cls, value: str) -> str:
        return _identifier(value, "observation_id")

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("observed_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "observed_at_utc")

    @model_validator(mode="after")
    def _validate_observation(self) -> Self:
        if self.observation_kind == "EXIT_QUOTE":
            if (
                self.bid_per_share is None
                or self.ask_per_share is None
                or self.underlying_price_usd_per_share is not None
            ):
                raise ValueError("exit quote requires only bid and ask")
            if self.bid_per_share < 0 or self.ask_per_share <= 0:
                raise ValueError("exit quote prices are invalid")
            if self.ask_per_share < self.bid_per_share:
                raise ValueError("exit quote cannot be crossed")
        elif (
            self.underlying_price_usd_per_share is None
            or self.underlying_price_usd_per_share <= 0
            or self.bid_per_share is not None
            or self.ask_per_share is not None
        ):
            raise ValueError("expiry settlement requires only a positive underlying price")
        return self

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"dq_report_bytes"})


class QQQOptionLifecycleExternalEvent(_DQBoundInput):
    event_id: str
    event_type: LifecycleExternalEventType
    option_sid: str
    occurred_at_utc: datetime
    effective_session: date
    contracts: int | None

    @field_validator("event_id")
    @classmethod
    def _validate_event_id(cls, value: str) -> str:
        return _identifier(value, "event_id")

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("occurred_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "occurred_at_utc")

    @model_validator(mode="after")
    def _validate_event(self) -> Self:
        exercise_like = self.event_type in {"EXERCISE", "ASSIGNMENT"}
        if exercise_like != (self.contracts is not None):
            raise ValueError("exercise/assignment alone require a contract quantity")
        if self.contracts is not None and self.contracts <= 0:
            raise ValueError("external event contracts must be positive")
        return self

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"dq_report_bytes"})


class QQQOptionPositionLifecycleRequest(_StrictModel):
    run_manifest_bytes: bytes
    run_manifest_file_sha256: str
    cash_accounting_result_bytes: bytes
    cash_accounting_result_file_sha256: str
    execution_artifacts: tuple[QQQOptionExecutionResultArtifact, ...]
    candidate_artifacts: tuple[QQQOptionCandidateSnapshotArtifact, ...]
    observations: tuple[QQQOptionLifecycleMarketObservation, ...]
    external_events: tuple[QQQOptionLifecycleExternalEvent, ...]
    evaluation_at_utc: datetime
    as_of_session: date
    exchange_sessions: tuple[date, ...]
    exchange_calendar_source_id: str
    exchange_calendar_source_sha256: str
    producer_version: str
    lineage_id: str

    @field_validator(
        "run_manifest_file_sha256",
        "cash_accounting_result_file_sha256",
        "exchange_calendar_source_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("evaluation_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "evaluation_at_utc")

    @field_validator("exchange_calendar_source_id", "producer_version", "lineage_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_request(self) -> Self:
        if _content_sha256(self.run_manifest_bytes) != self.run_manifest_file_sha256:
            raise ValueError("run manifest file hash differs from bytes")
        manifest = RunManifestRecord.from_json_bytes(self.run_manifest_bytes)
        if (
            _content_sha256(self.cash_accounting_result_bytes)
            != self.cash_accounting_result_file_sha256
        ):
            raise ValueError("cash accounting result file hash differs from bytes")
        QQQOptionCashAccountingResult.from_json_bytes(self.cash_accounting_result_bytes)
        if self.evaluation_at_utc < manifest.created_at_utc:
            raise ValueError("lifecycle evaluation cannot precede the run manifest")
        if not self.exchange_sessions:
            raise ValueError("reviewed exchange sessions cannot be empty")
        if self.exchange_sessions != tuple(sorted(self.exchange_sessions)):
            raise ValueError("exchange sessions must be sorted")
        if len(self.exchange_sessions) != len(set(self.exchange_sessions)):
            raise ValueError("exchange sessions must be unique")
        if any(item.weekday() >= 5 for item in self.exchange_sessions):
            raise ValueError("reviewed exchange sessions cannot include weekends")
        if self.as_of_session not in self.exchange_sessions:
            raise ValueError("as-of session must exist in the reviewed calendar")
        if self.evaluation_at_utc.astimezone(_EXCHANGE_TIMEZONE).date() != self.as_of_session:
            raise ValueError("evaluation UTC must resolve to the declared as-of session")
        execution_files = tuple(item.file_sha256 for item in self.execution_artifacts)
        if len(execution_files) != len(set(execution_files)):
            raise ValueError("execution artifact file identities must be unique")
        execution_contents = tuple(
            item.result.content_sha256 for item in self.execution_artifacts
        )
        if len(execution_contents) != len(set(execution_contents)):
            raise ValueError("execution result content identities must be unique")
        candidate_files = tuple(item.file_sha256 for item in self.candidate_artifacts)
        if len(candidate_files) != len(set(candidate_files)):
            raise ValueError("candidate artifact file identities must be unique")
        candidate_sids = tuple(item.candidate.option_sid for item in self.candidate_artifacts)
        if len(candidate_sids) != len(set(candidate_sids)):
            raise ValueError("candidate option SIDs must be unique")
        observation_ids = tuple(item.observation_id for item in self.observations)
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("lifecycle observation identities must be unique")
        event_ids = tuple(item.event_id for item in self.external_events)
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("external lifecycle event identities must be unique")
        source_ids = tuple(item.source_id for item in self.observations) + tuple(
            item.source_id for item in self.external_events
        )
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("lifecycle observation source ids must be unique")
        return self

    @property
    def run_manifest(self) -> RunManifestRecord:
        return RunManifestRecord.from_json_bytes(self.run_manifest_bytes)

    @property
    def accounting_result(self) -> QQQOptionCashAccountingResult:
        return QQQOptionCashAccountingResult.from_json_bytes(
            self.cash_accounting_result_bytes
        )


class QQQOptionLifecyclePositionSummary(_SealedModel):
    schema_version: Literal["qqq_option_lifecycle_position_summary.v1"]
    position_id: str
    option_sid: str
    right: Literal["CALL", "PUT"]
    expiry: date
    strike_usd_per_share: CanonicalDecimal
    contract_multiplier: int = Field(gt=0)
    terminal_state: PositionLifecycleState
    contracts_open: int = Field(ge=0)
    remaining_cost_basis_usd: CanonicalDecimal
    candidate_snapshot_sha256: str
    last_event_sha256: str

    @field_validator("position_id")
    @classmethod
    def _validate_position_id(cls, value: str) -> str:
        return _identifier(value, "position_id")

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("candidate_snapshot_sha256", "last_event_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_summary(self) -> Self:
        if self.strike_usd_per_share <= 0 or self.remaining_cost_basis_usd < 0:
            raise ValueError("position strike/cost is invalid")
        if self.terminal_state in {"FLAT", "CLOSED"} and (
            self.contracts_open != 0 or self.remaining_cost_basis_usd != 0
        ):
            raise ValueError("flat/closed position cannot retain quantity or cost")
        if self.terminal_state in {"OPEN", "OPEN_PARTIAL", "EXIT_PENDING", "EXIT_BLOCKED"} and (
            self.contracts_open <= 0
        ):
            raise ValueError("open lifecycle state requires positive quantity")
        return self


class QQQOptionPositionLifecycleResult(_SealedModel):
    schema_version: Literal["qqq_options_position_lifecycle_result.v1"]
    policy_sha256: str
    selection_policy_sha256: str
    execution_policy_sha256: str
    accounting_policy_sha256: str
    accounting_result_sha256: str
    input_sha256: str
    lifecycle_authorized: bool
    investment_interpretation_allowed: bool
    cash_preservation_required: bool
    run_valid: bool
    reason_code: LifecycleReason
    lifecycle_stage_dq_status: DQStatus
    lifecycle_stage_pit_status: DQStatus
    lifecycle_events: tuple[PositionLifecycleEventRecord, ...]
    positions: tuple[QQQOptionLifecyclePositionSummary, ...]
    portfolio_snapshot: PortfolioSnapshotRecord | None
    new_order_intent_count: Literal[0]
    new_fill_count: Literal[0]
    safety: QQQOptionPositionLifecycleSafety

    @field_validator(
        "policy_sha256",
        "selection_policy_sha256",
        "execution_policy_sha256",
        "accounting_policy_sha256",
        "accounting_result_sha256",
        "input_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        for event in self.lifecycle_events:
            PositionLifecycleEventRecord.from_json_bytes(event.canonical_bytes)
        for position in self.positions:
            QQQOptionLifecyclePositionSummary.from_json_bytes(position.canonical_bytes)
        if self.portfolio_snapshot is not None:
            PortfolioSnapshotRecord.from_json_bytes(self.portfolio_snapshot.canonical_bytes)
        event_hashes = tuple(item.content_sha256 for item in self.lifecycle_events)
        if len(event_hashes) != len(set(event_hashes)):
            raise ValueError("lifecycle event content identities must be unique")
        position_ids = tuple(item.position_id for item in self.positions)
        if position_ids != tuple(sorted(position_ids)) or len(position_ids) != len(
            set(position_ids)
        ):
            raise ValueError("position summaries must be sorted and unique")
        by_position: dict[str, list[int]] = {}
        for event in self.lifecycle_events:
            by_position.setdefault(event.position_id, []).append(event.event_sequence)
        if any(values != list(range(len(values))) for values in by_position.values()):
            raise ValueError("lifecycle event sequences must be contiguous by position")
        blocked_without_state = self.reason_code in {
            "LIFECYCLE_POLICY_REVIEW_REQUIRED",
            "ACCOUNTING_REPLAY_BLOCKED_CASH_PRESERVED",
            "LIFECYCLE_INPUT_INVALID",
        }
        if blocked_without_state and (
            self.lifecycle_events or self.positions or self.portfolio_snapshot is not None
        ):
            raise ValueError("pre-admission blocked result cannot expose partial state")
        if not self.lifecycle_authorized and not blocked_without_state:
            raise ValueError("unauthorized lifecycle cannot expose replay state")
        invalid = self.reason_code in {
            "LIFECYCLE_INPUT_INVALID",
            "EXPIRY_SCOPE_VIOLATION_INVALID_RUN",
            "EXTERNAL_SCOPE_VIOLATION_INVALID_RUN",
            "UNRESOLVED_EXPIRY_INVALID_RUN",
        }
        if (blocked_without_state or invalid) and self.run_valid:
            raise ValueError("run validity differs from lifecycle disposition")
        if not blocked_without_state and not invalid and not self.run_valid:
            raise ValueError("run validity differs from lifecycle disposition")
        if invalid and self.portfolio_snapshot is not None:
            raise ValueError("invalid lifecycle cannot publish a downstream snapshot")
        if self.run_valid and self.portfolio_snapshot is None:
            raise ValueError("valid active lifecycle requires a downstream snapshot")
        if self.investment_interpretation_allowed and not (
            self.lifecycle_authorized
            and self.run_valid
            and self.lifecycle_stage_dq_status == "PASS"
            and self.lifecycle_stage_pit_status == "PASS"
            and self.portfolio_snapshot is not None
        ):
            raise ValueError("investment interpretation requires a fully valid lifecycle")
        if blocked_without_state and not self.cash_preservation_required:
            raise ValueError("blocked lifecycle must preserve cash")
        return self


@dataclass
class _PositionState:
    position_id: str
    candidate_artifact: QQQOptionCandidateSnapshotArtifact
    state: PositionLifecycleState = "FLAT"
    contracts_open: int = 0
    remaining_cost_basis_usd: Decimal = Decimal("0")
    opened_at_utc: datetime | None = None
    events: list[PositionLifecycleEventRecord] = field(default_factory=list)

    @property
    def candidate(self) -> ContractCandidateSnapshotRecord:
        return self.candidate_artifact.candidate


def load_qqq_options_position_lifecycle_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionPositionLifecyclePolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionPositionLifecyclePolicy.model_validate(payload, strict=False)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionPositionLifecycleContractError(
            "QQQ_OPTION_LIFECYCLE_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionPositionLifecyclePolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_content_sha256(content),
    )


def build_qqq_option_position_lifecycle_input_sha256(
    request: QQQOptionPositionLifecycleRequest,
) -> str:
    execution_rows = sorted(
        (
            {
                "file_sha256": item.file_sha256,
                "content_sha256": item.result.content_sha256,
            }
            for item in request.execution_artifacts
        ),
        key=lambda item: (item["content_sha256"], item["file_sha256"]),
    )
    candidate_rows = sorted(
        (
            {
                "file_sha256": item.file_sha256,
                "content_sha256": item.candidate.content_sha256,
                "option_sid": item.candidate.option_sid,
            }
            for item in request.candidate_artifacts
        ),
        key=lambda item: item["option_sid"],
    )
    payload = {
        "schema_version": "qqq_options_position_lifecycle_input.v1",
        "run_manifest_file_sha256": request.run_manifest_file_sha256,
        "cash_accounting_result_file_sha256": (
            request.cash_accounting_result_file_sha256
        ),
        "execution_artifacts": execution_rows,
        "candidate_artifacts": candidate_rows,
        "observations": sorted(
            (item.identity_payload() for item in request.observations),
            key=lambda item: item["observation_id"],
        ),
        "external_events": sorted(
            (item.identity_payload() for item in request.external_events),
            key=lambda item: item["event_id"],
        ),
        "evaluation_at_utc": request.evaluation_at_utc.isoformat(),
        "as_of_session": request.as_of_session.isoformat(),
        "exchange_sessions": [item.isoformat() for item in request.exchange_sessions],
        "exchange_calendar_source_id": request.exchange_calendar_source_id,
        "exchange_calendar_source_sha256": request.exchange_calendar_source_sha256,
        "producer_version": request.producer_version,
        "lineage_id": request.lineage_id,
    }
    return _content_sha256(_canonical_json_bytes(payload))


def _blocked_result(
    *,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    accounting_result_sha256: str,
    input_sha256: str,
    reason_code: LifecycleReason,
    authorized: bool,
) -> QQQOptionPositionLifecycleResult:
    return QQQOptionPositionLifecycleResult.seal(
        schema_version="qqq_options_position_lifecycle_result.v1",
        policy_sha256=loaded.policy_sha256,
        selection_policy_sha256=loaded.policy.selection_policy_sha256,
        execution_policy_sha256=loaded.policy.execution_policy_sha256,
        accounting_policy_sha256=loaded.policy.accounting_policy_sha256,
        accounting_result_sha256=accounting_result_sha256,
        input_sha256=input_sha256,
        lifecycle_authorized=authorized,
        investment_interpretation_allowed=False,
        cash_preservation_required=True,
        run_valid=False,
        reason_code=reason_code,
        lifecycle_stage_dq_status="NOT_EVALUATED",
        lifecycle_stage_pit_status="NOT_EVALUATED",
        lifecycle_events=(),
        positions=(),
        portfolio_snapshot=None,
        new_order_intent_count=0,
        new_fill_count=0,
        safety=loaded.policy.safety,
    )


def _source_map(
    record: PortfolioSnapshotRecord | ContractCandidateSnapshotRecord,
) -> dict[str, str]:
    return dict(zip(record.source_ids, record.source_checksums, strict=True))


def _validate_record_identity(
    record: PortfolioSnapshotRecord | ContractCandidateSnapshotRecord,
    manifest: RunManifestRecord,
) -> None:
    if (
        record.run_id != manifest.run_id
        or record.repository_code_sha != manifest.repository_code_sha
        or record.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256
        or record.requested_start != manifest.requested_start
        or record.requested_end != manifest.requested_end
        or record.evaluated_start != manifest.evaluated_start
        or record.evaluated_end != manifest.evaluated_end
    ):
        raise _ReplayAbort("record run/code/contract/range differs from manifest")


def _validate_dq_bound_input(
    value: _DQBoundInput,
    *,
    manifest: RunManifestRecord,
    policy: QQQOptionPositionLifecyclePolicy,
    evaluation_at_utc: datetime,
) -> None:
    report = value.dq_report
    _validate_record_identity(report, manifest)  # type: ignore[arg-type]
    if report.policy_sha256 != policy.dq_pit_policy_sha256:
        raise _ReplayAbort("lifecycle observation DQ policy differs from authority")
    if report.scope != "qqq_options_event_dq_pit_identity":
        raise _ReplayAbort("lifecycle observation DQ scope is not canonical")
    sources = dict(zip(report.source_ids, report.source_checksums, strict=True))
    if sources.get(value.source_id) != value.source_sha256:
        raise _ReplayAbort("lifecycle observation source is absent from its DQ report")
    if report.generated_at_utc > evaluation_at_utc or report.created_at_utc > evaluation_at_utc:
        raise _ReplayAbort("lifecycle observation DQ report is from the future")


def _validate_replay_identity(
    request: QQQOptionPositionLifecycleRequest,
    *,
    policy: QQQOptionPositionLifecyclePolicy,
) -> None:
    manifest = request.run_manifest
    accounting = request.accounting_result
    if manifest.requested_start != date(2021, 2, 22) or manifest.evaluated_start != date(
        2021, 2, 22
    ):
        raise _ReplayAbort("primary requested/evaluated start must remain 2021-02-22")
    if accounting.policy_sha256 != policy.accounting_policy_sha256:
        raise _ReplayAbort("accounting result policy differs from lifecycle authority")
    if accounting.execution_policy_sha256 != policy.execution_policy_sha256:
        raise _ReplayAbort("accounting execution policy differs from lifecycle authority")
    snapshot = accounting.portfolio_snapshot
    if snapshot is None:
        raise _ReplayAbort("ready lifecycle requires an accounting portfolio snapshot")
    _validate_record_identity(snapshot, manifest)
    if snapshot.created_at_utc > request.evaluation_at_utc:
        raise _ReplayAbort("lifecycle evaluation precedes the accounting snapshot")
    sources = _source_map(snapshot)
    if sources.get("qqq.options.accounting.run_manifest") != request.run_manifest_file_sha256:
        raise _ReplayAbort("accounting snapshot run-manifest file identity drifted")
    if sources.get("qqq.options.accounting.calendar") != (
        request.exchange_calendar_source_sha256
    ):
        raise _ReplayAbort("accounting snapshot calendar identity drifted")
    snapshot_execution_files = sorted(
        checksum
        for source_id, checksum in sources.items()
        if source_id.startswith("qqq.options.accounting.execution.")
    )
    request_execution_files = sorted(
        item.file_sha256 for item in request.execution_artifacts
    )
    if snapshot_execution_files != request_execution_files:
        raise _ReplayAbort("accounting snapshot execution artifact inventory drifted")
    intent_sids: set[str] = set()
    for artifact in request.execution_artifacts:
        result = artifact.result
        if result.policy_sha256 != policy.execution_policy_sha256:
            raise _ReplayAbort("execution result policy differs from lifecycle authority")
        if result.selection_policy_sha256 != policy.selection_policy_sha256:
            raise _ReplayAbort("execution selection policy differs from lifecycle authority")
        if result.accounting_status != "NOT_EVALUATED":
            raise _ReplayAbort("execution result pre-asserts accounting status")
        if result.order_intent is not None:
            intent_sids.add(result.order_intent.option_sid)
        records = (
            (() if result.order_intent is None else (result.order_intent,))
            + result.order_events
            + result.fill_events
        )
        for record in records:
            _validate_record_identity(record, manifest)  # type: ignore[arg-type]
            if record.policy_sha256 != policy.execution_policy_sha256:
                raise _ReplayAbort("execution record policy lineage drifted")
    candidates = {item.candidate.option_sid: item for item in request.candidate_artifacts}
    if set(candidates) != intent_sids:
        raise _ReplayAbort("candidate SID inventory must exactly match execution intents")
    for artifact in request.candidate_artifacts:
        candidate = artifact.candidate
        _validate_record_identity(candidate, manifest)
        if candidate.policy_sha256 != policy.selection_policy_sha256:
            raise _ReplayAbort("candidate selection policy lineage drifted")
        if not candidate.eligible:
            raise _ReplayAbort("lifecycle cannot admit an ineligible candidate")
    for value in (*request.observations, *request.external_events):
        _validate_dq_bound_input(
            value,
            manifest=manifest,
            policy=policy,
            evaluation_at_utc=request.evaluation_at_utc,
        )


def _result_sort_key(
    artifact: QQQOptionExecutionResultArtifact,
) -> tuple[str, str, str]:
    result = artifact.result
    if result.order_intent is None:
        return ("9999-12-31T23:59:59+00:00", "zzzz", result.content_sha256)
    return (
        result.order_intent.created_at_utc.isoformat(),
        result.order_intent.intent_id,
        result.content_sha256,
    )


def _merge_sources(pairs: list[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    result: dict[str, str] = {}
    for source_id, checksum in pairs:
        prior = result.setdefault(source_id, checksum)
        if prior != checksum:
            raise _ReplayAbort("one lifecycle source id has conflicting checksums")
    return tuple(sorted(result.items()))


def _event_statuses(
    candidate: ContractCandidateSnapshotRecord,
    accounting_snapshot: PortfolioSnapshotRecord,
    result: QQQOptionExecutionResult | None,
) -> tuple[DQStatus, DQStatus]:
    dq: list[DQStatus] = [candidate.dq_status, accounting_snapshot.dq_status]
    pit: list[DQStatus] = [candidate.pit_status, accounting_snapshot.pit_status]
    if result is not None:
        dq.extend([result.global_dq_status, result.execution_stage_dq_status])
        pit.append(result.global_pit_status)
        if result.order_intent is not None:
            dq.append(result.order_intent.dq_status)
            pit.append(result.order_intent.pit_status)
        dq.extend(item.dq_status for item in result.order_events)
        pit.extend(item.pit_status for item in result.order_events)
        dq.extend(item.dq_status for item in result.fill_events)
        pit.extend(item.pit_status for item in result.fill_events)
    return _worst_status(dq), _worst_status(pit)


def _append_event(
    state: _PositionState,
    *,
    request: QQQOptionPositionLifecycleRequest,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    input_sha256: str,
    prior_state: PositionLifecycleState,
    next_state: PositionLifecycleState,
    occurred_at_utc: datetime,
    quantity_delta_contracts: int,
    cash_delta_usd: Decimal,
    reason_code: str,
    extra_sources: list[tuple[str, str]],
    dq_status: DQStatus,
    pit_status: DQStatus,
) -> PositionLifecycleEventRecord:
    if state.state != prior_state:
        raise _ReplayAbort("lifecycle replay prior state drifted")
    if occurred_at_utc > request.evaluation_at_utc:
        raise _ReplayAbort("lifecycle event occurs after evaluation")
    sources = _merge_sources(
        [
            ("qqq.options.lifecycle.input", input_sha256),
            (
                "qqq.options.lifecycle.accounting_file",
                request.cash_accounting_result_file_sha256,
            ),
            (
                "qqq.options.lifecycle.candidate_file",
                state.candidate_artifact.file_sha256,
            ),
        ]
        + extra_sources
    )
    event = PositionLifecycleEventRecord.seal(
        schema_name="position_lifecycle_event",
        schema_version="1.0.0",
        run_id=request.run_manifest.run_id,
        record_id=f"{state.position_id}.event.{len(state.events):04d}",
        created_at_utc=request.evaluation_at_utc,
        producer_version=request.producer_version,
        repository_code_sha=request.run_manifest.repository_code_sha,
        policy_id=loaded.policy.policy_id,
        policy_version=loaded.policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(item[0] for item in sources),
        source_checksums=tuple(item[1] for item in sources),
        requested_start=request.run_manifest.requested_start,
        requested_end=request.run_manifest.requested_end,
        evaluated_start=request.run_manifest.evaluated_start,
        evaluated_end=request.run_manifest.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=request.lineage_id,
        safety=_shared_safety(),
        position_id=state.position_id,
        event_sequence=len(state.events),
        occurred_at_utc=occurred_at_utc,
        prior_state=prior_state,
        next_state=next_state,
        quantity_delta_contracts=quantity_delta_contracts,
        cash_delta_usd=cash_delta_usd,
        reason_code=reason_code,
    )
    state.events.append(event)
    state.state = next_state
    state.contracts_open += quantity_delta_contracts
    if state.contracts_open < 0:
        raise _ReplayAbort("lifecycle quantity became negative")
    return event


def _position_id(input_sha256: str, option_sid: str, cycle: int) -> str:
    sid_hash = _content_sha256(option_sid.encode("utf-8"))[:12]
    return f"lifecycle.{input_sha256[:16]}.{sid_hash}.{cycle:04d}"


def _replay_execution_states(
    request: QQQOptionPositionLifecycleRequest,
    *,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    input_sha256: str,
) -> tuple[list[_PositionState], dict[str, _PositionState]]:
    accounting = request.accounting_result
    snapshot = accounting.portfolio_snapshot
    assert snapshot is not None
    candidates = {item.candidate.option_sid: item for item in request.candidate_artifacts}
    current: dict[str, _PositionState] = {}
    all_states: list[_PositionState] = []
    cycles: dict[str, int] = {}
    for artifact in sorted(request.execution_artifacts, key=_result_sort_key):
        result = artifact.result
        intent = result.order_intent
        if intent is None:
            continue
        candidate_artifact = candidates[intent.option_sid]
        candidate = candidate_artifact.candidate
        if any(
            fill.contract_multiplier != candidate.contract_multiplier
            for fill in result.fill_events
        ):
            raise _ReplayAbort("execution fill multiplier differs from candidate")
        dq_status, pit_status = _event_statuses(candidate, snapshot, result)
        source_pairs = [
            ("qqq.options.lifecycle.execution_file", artifact.file_sha256),
            ("qqq.options.lifecycle.execution_result", result.content_sha256),
            ("qqq.options.lifecycle.intent", intent.content_sha256),
        ]
        if intent.side == "BUY_TO_OPEN":
            prior = current.get(intent.option_sid)
            if prior is not None and prior.contracts_open > 0:
                raise _ReplayAbort("scale-in while a lifecycle position is open is unsupported")
            cycle = cycles.get(intent.option_sid, 0)
            cycles[intent.option_sid] = cycle + 1
            state = _PositionState(
                position_id=_position_id(input_sha256, intent.option_sid, cycle),
                candidate_artifact=candidate_artifact,
            )
            current[intent.option_sid] = state
            all_states.append(state)
            _append_event(
                state,
                request=request,
                loaded=loaded,
                input_sha256=input_sha256,
                prior_state="FLAT",
                next_state="INTENT_PENDING",
                occurred_at_utc=intent.created_at_utc,
                quantity_delta_contracts=0,
                cash_delta_usd=Decimal("0"),
                reason_code="OPEN_INTENT_CREATED",
                extra_sources=source_pairs,
                dq_status=dq_status,
                pit_status=pit_status,
            )
            if not result.order_events:
                raise _ReplayAbort("canonical execution intent lacks order events")
            total_filled = sum(item.filled_contracts for item in result.fill_events)
            terminal_time = result.order_events[-1].event_at_utc
            cash_delta = sum(
                (item.gross_cash_delta_usd - item.fee_usd for item in result.fill_events),
                Decimal("0"),
            )
            terminal_sources = source_pairs + [
                (
                    f"qqq.options.lifecycle.order_event.{item.event_sequence:04d}",
                    item.content_sha256,
                )
                for item in result.order_events
            ] + [
                (f"qqq.options.lifecycle.fill_event.{item.fill_sequence:04d}", item.content_sha256)
                for item in result.fill_events
            ]
            if total_filled == 0:
                next_state: PositionLifecycleState = "FLAT"
                reason = "OPEN_NO_FILL_TERMINAL"
            elif total_filled == intent.contracts:
                next_state = "OPEN"
                reason = "OPEN_FULL_FILL"
                state.opened_at_utc = terminal_time
            else:
                next_state = "OPEN_PARTIAL"
                reason = "OPEN_PARTIAL_FILL"
                state.opened_at_utc = terminal_time
            _append_event(
                state,
                request=request,
                loaded=loaded,
                input_sha256=input_sha256,
                prior_state="INTENT_PENDING",
                next_state=next_state,
                occurred_at_utc=terminal_time,
                quantity_delta_contracts=total_filled,
                cash_delta_usd=cash_delta,
                reason_code=reason,
                extra_sources=terminal_sources,
                dq_status=dq_status,
                pit_status=pit_status,
            )
            continue
        state = current.get(intent.option_sid)
        if state is None or state.contracts_open <= 0:
            raise _ReplayAbort("SELL_TO_CLOSE lacks an open lifecycle position")
        if intent.contracts > state.contracts_open:
            raise _ReplayAbort("SELL_TO_CLOSE intent exceeds lifecycle quantity")
        if state.state in {"OPEN", "OPEN_PARTIAL"}:
            _append_event(
                state,
                request=request,
                loaded=loaded,
                input_sha256=input_sha256,
                prior_state=state.state,
                next_state="EXIT_PENDING",
                occurred_at_utc=intent.created_at_utc,
                quantity_delta_contracts=0,
                cash_delta_usd=Decimal("0"),
                reason_code="EXIT_INTENT_CREATED",
                extra_sources=source_pairs,
                dq_status=dq_status,
                pit_status=pit_status,
            )
        elif state.state != "EXIT_BLOCKED":
            raise _ReplayAbort("SELL_TO_CLOSE starts from an illegal lifecycle state")
        if not result.order_events:
            raise _ReplayAbort("canonical exit intent lacks order events")
        total_filled = sum(item.filled_contracts for item in result.fill_events)
        terminal_time = result.order_events[-1].event_at_utc
        cash_delta = sum(
            (item.gross_cash_delta_usd - item.fee_usd for item in result.fill_events),
            Decimal("0"),
        )
        terminal_sources = source_pairs + [
            (f"qqq.options.lifecycle.order_event.{item.event_sequence:04d}", item.content_sha256)
            for item in result.order_events
        ] + [
            (f"qqq.options.lifecycle.fill_event.{item.fill_sequence:04d}", item.content_sha256)
            for item in result.fill_events
        ]
        closes = total_filled == state.contracts_open
        if state.state == "EXIT_BLOCKED" and not closes:
            if total_filled:
                raise _ReplayAbort("partial retry from EXIT_BLOCKED is outside V1 transitions")
            continue
        next_state = "CLOSED" if closes else "EXIT_BLOCKED"
        reason = "EXIT_FULL_FILL" if closes else "EXIT_PARTIAL_OR_NO_FILL"
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state=state.state,
            next_state=next_state,
            occurred_at_utc=terminal_time,
            quantity_delta_contracts=-total_filled,
            cash_delta_usd=cash_delta,
            reason_code=reason,
            extra_sources=terminal_sources,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    actual_positions = {item.option_sid: item for item in accounting.positions}
    replay_quantities = {
        sid: state.contracts_open
        for sid, state in current.items()
        if state.contracts_open > 0
    }
    if replay_quantities != {
        sid: position.contracts_open for sid, position in actual_positions.items()
    }:
        raise _ReplayAbort("lifecycle quantities differ from accounting positions")
    for sid, position in actual_positions.items():
        state = current[sid]
        if position.contract_multiplier != state.candidate.contract_multiplier:
            raise _ReplayAbort("accounting position multiplier differs from candidate")
        state.remaining_cost_basis_usd = position.remaining_cost_basis_usd
    return all_states, current


def _observation_is_pass(value: _DQBoundInput) -> bool:
    report = value.dq_report
    return report.dq_status == "PASS" and report.pit_status == "PASS"


def _scope_invalid(
    state: _PositionState,
    *,
    request: QQQOptionPositionLifecycleRequest,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    input_sha256: str,
    occurred_at_utc: datetime,
    reason_code: str,
    source_pairs: list[tuple[str, str]],
    dq_status: DQStatus,
    pit_status: DQStatus,
) -> None:
    if state.state == "OPEN":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="OPEN",
            next_state="SCOPE_VIOLATION",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code=reason_code,
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    elif state.state == "OPEN_PARTIAL":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="OPEN_PARTIAL",
            next_state="EXIT_PENDING",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code=f"{reason_code}_GUARD",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    if state.state == "EXIT_PENDING":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="EXIT_PENDING",
            next_state="EXIT_BLOCKED",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code=f"{reason_code}_BLOCKED",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    if state.state == "SCOPE_VIOLATION":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="SCOPE_VIOLATION",
            next_state="INVALID_RUN",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code="UNDERLYING_EXPOSURE_PROHIBITED",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    elif state.state == "EXIT_BLOCKED":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="EXIT_BLOCKED",
            next_state="INVALID_RUN",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code="UNRESOLVED_SCOPE_VIOLATION",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )


def _invalid_unresolved_expiry(
    state: _PositionState,
    *,
    request: QQQOptionPositionLifecycleRequest,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    input_sha256: str,
    occurred_at_utc: datetime,
    source_pairs: list[tuple[str, str]],
    dq_status: DQStatus,
    pit_status: DQStatus,
) -> None:
    if state.state in {"OPEN", "OPEN_PARTIAL"}:
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state=state.state,
            next_state="EXIT_PENDING",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code="EXPIRY_EXIT_REQUIRED",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    if state.state == "EXIT_PENDING":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="EXIT_PENDING",
            next_state="EXIT_BLOCKED",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code="EXPIRY_SETTLEMENT_UNRESOLVED",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
    if state.state == "EXIT_BLOCKED":
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state="EXIT_BLOCKED",
            next_state="INVALID_RUN",
            occurred_at_utc=occurred_at_utc,
            quantity_delta_contracts=0,
            cash_delta_usd=Decimal("0"),
            reason_code="UNRESOLVED_EXPIRY_INVALID_RUN",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )


def _apply_external_and_expiry_safety(
    request: QQQOptionPositionLifecycleRequest,
    *,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    input_sha256: str,
    all_states: list[_PositionState],
    current: dict[str, _PositionState],
) -> tuple[LifecycleReason, list[QQQOptionAccountingPosition]]:
    criteria = loaded.policy.criteria
    if not isinstance(criteria, ActivePositionLifecycleCriteria):
        raise _ReplayAbort("authorized lifecycle policy lacks active criteria")
    snapshot = request.accounting_result.portfolio_snapshot
    assert snapshot is not None
    accounting_positions = {
        item.option_sid: item for item in request.accounting_result.positions
    }
    expired_positions: list[QQQOptionAccountingPosition] = []
    external_invalid = False
    expiry_invalid = False
    unresolved_expiry = False
    pre_expiry_required = False
    pre_expiry_blocked = False
    observations_by_key: dict[
        tuple[str, LifecycleObservationKind], list[QQQOptionLifecycleMarketObservation]
    ] = {}
    for observation in request.observations:
        observations_by_key.setdefault(
            (observation.option_sid, observation.observation_kind), []
        ).append(observation)
    for values in observations_by_key.values():
        values.sort(
            key=lambda item: (
                item.effective_session,
                item.observed_at_utc,
                item.observation_id,
            )
        )
    for event in sorted(
        request.external_events,
        key=lambda item: (item.occurred_at_utc, item.event_id),
    ):
        state = current.get(event.option_sid)
        if (
            state is None
            or state.contracts_open <= 0
            or state.state in {"CLOSED", "FLAT", "INVALID_RUN"}
            or (state.opened_at_utc is not None and event.occurred_at_utc < state.opened_at_utc)
            or event.occurred_at_utc > request.evaluation_at_utc
        ):
            continue
        report = event.dq_report
        dq_status = _worst_status([snapshot.dq_status, report.dq_status])
        pit_status = _worst_status([snapshot.pit_status, report.pit_status])
        source_pairs = [
            ("qqq.options.lifecycle.external_event", event.source_sha256),
            ("qqq.options.lifecycle.external_event_dq", event.dq_report_file_sha256),
        ]
        reason = (
            "UNEXPECTED_EXERCISE"
            if event.event_type == "EXERCISE"
            else "UNEXPECTED_ASSIGNMENT"
            if event.event_type == "ASSIGNMENT"
            else "CORPORATE_ACTION_UNMODELED"
        )
        _scope_invalid(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            occurred_at_utc=event.occurred_at_utc,
            reason_code=reason,
            source_pairs=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
        external_invalid = True
    session_index = {session: index for index, session in enumerate(request.exchange_sessions)}
    for state in all_states:
        if state.contracts_open <= 0 or state.state == "INVALID_RUN":
            continue
        candidate = state.candidate
        if candidate.expiry not in session_index:
            raise _ReplayAbort("reviewed calendar does not contain candidate expiry")
        expiry_index = session_index[candidate.expiry]
        as_of_index = session_index[request.as_of_session]
        if request.as_of_session < candidate.expiry:
            sessions_to_expiry = expiry_index - as_of_index
            if sessions_to_expiry > criteria.pre_expiry_guard_sessions:
                continue
            if state.state in {"OPEN", "OPEN_PARTIAL"}:
                _append_event(
                    state,
                    request=request,
                    loaded=loaded,
                    input_sha256=input_sha256,
                    prior_state=state.state,
                    next_state="EXIT_PENDING",
                    occurred_at_utc=request.evaluation_at_utc,
                    quantity_delta_contracts=0,
                    cash_delta_usd=Decimal("0"),
                    reason_code="PRE_EXPIRY_GUARD_TRIGGERED",
                    extra_sources=[
                        (
                            "qqq.options.lifecycle.calendar",
                            request.exchange_calendar_source_sha256,
                        )
                    ],
                    dq_status=snapshot.dq_status,
                    pit_status=snapshot.pit_status,
                )
            pre_expiry_required = True
            quotes = [
                item
                for item in observations_by_key.get((candidate.option_sid, "EXIT_QUOTE"), [])
                if item.effective_session == request.as_of_session
                and item.observed_at_utc <= request.evaluation_at_utc
            ]
            quote = quotes[-1] if quotes else None
            quote_valid = bool(
                quote is not None
                and _observation_is_pass(quote)
                and request.evaluation_at_utc - quote.observed_at_utc
                <= timedelta(milliseconds=criteria.max_exit_quote_age_ms)
            )
            if not quote_valid and state.state == "EXIT_PENDING":
                source_pairs = [
                    (
                        "qqq.options.lifecycle.calendar",
                        request.exchange_calendar_source_sha256,
                    )
                ]
                dq_status = snapshot.dq_status
                pit_status = snapshot.pit_status
                if quote is not None:
                    source_pairs.extend(
                        [
                            ("qqq.options.lifecycle.exit_quote", quote.source_sha256),
                            (
                                "qqq.options.lifecycle.exit_quote_dq",
                                quote.dq_report_file_sha256,
                            ),
                        ]
                    )
                    dq_status = _worst_status([dq_status, quote.dq_report.dq_status])
                    pit_status = _worst_status([pit_status, quote.dq_report.pit_status])
                _append_event(
                    state,
                    request=request,
                    loaded=loaded,
                    input_sha256=input_sha256,
                    prior_state="EXIT_PENDING",
                    next_state="EXIT_BLOCKED",
                    occurred_at_utc=request.evaluation_at_utc,
                    quantity_delta_contracts=0,
                    cash_delta_usd=Decimal("0"),
                    reason_code="PRE_EXPIRY_EXIT_QUOTE_UNAVAILABLE",
                    extra_sources=source_pairs,
                    dq_status=dq_status,
                    pit_status=pit_status,
                )
                pre_expiry_blocked = True
            continue
        settlements = [
            item
            for item in observations_by_key.get((candidate.option_sid, "EXPIRY_SETTLEMENT"), [])
            if item.effective_session == candidate.expiry
            and item.observed_at_utc <= request.evaluation_at_utc
        ]
        settlement = settlements[-1] if settlements else None
        if settlement is None or not _observation_is_pass(settlement):
            source_pairs = [
                ("qqq.options.lifecycle.calendar", request.exchange_calendar_source_sha256)
            ]
            dq_status = snapshot.dq_status
            pit_status = snapshot.pit_status
            if settlement is not None:
                source_pairs.extend(
                    [
                        ("qqq.options.lifecycle.expiry_settlement", settlement.source_sha256),
                        (
                            "qqq.options.lifecycle.expiry_settlement_dq",
                            settlement.dq_report_file_sha256,
                        ),
                    ]
                )
                dq_status = _worst_status([dq_status, settlement.dq_report.dq_status])
                pit_status = _worst_status([pit_status, settlement.dq_report.pit_status])
            _invalid_unresolved_expiry(
                state,
                request=request,
                loaded=loaded,
                input_sha256=input_sha256,
                occurred_at_utc=request.evaluation_at_utc,
                source_pairs=source_pairs,
                dq_status=dq_status,
                pit_status=pit_status,
            )
            unresolved_expiry = True
            continue
        assert settlement.underlying_price_usd_per_share is not None
        intrinsic = (
            settlement.underlying_price_usd_per_share
            - candidate.strike_usd_per_share
            if candidate.right == "CALL"
            else candidate.strike_usd_per_share
            - settlement.underlying_price_usd_per_share
        )
        source_pairs = [
            ("qqq.options.lifecycle.expiry_settlement", settlement.source_sha256),
            (
                "qqq.options.lifecycle.expiry_settlement_dq",
                settlement.dq_report_file_sha256,
            ),
            ("qqq.options.lifecycle.calendar", request.exchange_calendar_source_sha256),
        ]
        dq_status = _worst_status([snapshot.dq_status, settlement.dq_report.dq_status])
        pit_status = _worst_status([snapshot.pit_status, settlement.dq_report.pit_status])
        if intrinsic > 0:
            _scope_invalid(
                state,
                request=request,
                loaded=loaded,
                input_sha256=input_sha256,
                occurred_at_utc=request.evaluation_at_utc,
                reason_code="ITM_EXPIRY_UNDERLYING_EXPOSURE",
                source_pairs=source_pairs,
                dq_status=dq_status,
                pit_status=pit_status,
            )
            expiry_invalid = True
            continue
        if state.state in {"OPEN", "OPEN_PARTIAL"}:
            _append_event(
                state,
                request=request,
                loaded=loaded,
                input_sha256=input_sha256,
                prior_state=state.state,
                next_state="EXIT_PENDING",
                occurred_at_utc=request.evaluation_at_utc,
                quantity_delta_contracts=0,
                cash_delta_usd=Decimal("0"),
                reason_code="EXPIRY_PROCESSING_STARTED",
                extra_sources=source_pairs,
                dq_status=dq_status,
                pit_status=pit_status,
            )
        if state.state not in {"EXIT_PENDING", "EXIT_BLOCKED"}:
            raise _ReplayAbort("worthless expiry starts from an illegal lifecycle state")
        position = accounting_positions.get(candidate.option_sid)
        if position is None:
            raise _ReplayAbort("expiring lifecycle position is absent from accounting")
        _append_event(
            state,
            request=request,
            loaded=loaded,
            input_sha256=input_sha256,
            prior_state=state.state,
            next_state="CLOSED",
            occurred_at_utc=request.evaluation_at_utc,
            quantity_delta_contracts=-state.contracts_open,
            cash_delta_usd=Decimal("0"),
            reason_code="EXPIRED_WORTHLESS",
            extra_sources=source_pairs,
            dq_status=dq_status,
            pit_status=pit_status,
        )
        state.remaining_cost_basis_usd = Decimal("0")
        expired_positions.append(position)
    if external_invalid:
        return "EXTERNAL_SCOPE_VIOLATION_INVALID_RUN", expired_positions
    if expiry_invalid:
        return "EXPIRY_SCOPE_VIOLATION_INVALID_RUN", expired_positions
    if unresolved_expiry:
        return "UNRESOLVED_EXPIRY_INVALID_RUN", expired_positions
    if expired_positions:
        return "EXPIRY_CLOSED_WORTHLESS", expired_positions
    if pre_expiry_blocked:
        return "PRE_EXPIRY_EXIT_BLOCKED", expired_positions
    if pre_expiry_required:
        return "PRE_EXPIRY_EXIT_REQUIRED", expired_positions
    return "LIFECYCLE_REPLAY_READY", expired_positions


def _build_downstream_snapshot(
    request: QQQOptionPositionLifecycleRequest,
    *,
    loaded: QQQOptionPositionLifecyclePolicyLoadResult,
    input_sha256: str,
    expired_positions: list[QQQOptionAccountingPosition],
    events: tuple[PositionLifecycleEventRecord, ...],
) -> PortfolioSnapshotRecord:
    prior = request.accounting_result.portfolio_snapshot
    assert prior is not None
    market_value_removed = sum(
        (item.liquidation_value_usd for item in expired_positions), Decimal("0")
    )
    unrealized_removed = sum(
        (item.unrealized_pnl_usd for item in expired_positions), Decimal("0")
    )
    cost_expired = sum(
        (item.remaining_cost_basis_usd for item in expired_positions), Decimal("0")
    )
    option_market_value = prior.option_market_value_usd - market_value_removed
    if option_market_value < 0:
        raise _ReplayAbort("expiry adjustment made option market value negative")
    dq_status = _worst_status(
        [prior.dq_status] + [item.dq_status for item in events]
    )
    pit_status = _worst_status(
        [prior.pit_status] + [item.pit_status for item in events]
    )
    sources = _merge_sources(
        [
            ("qqq.options.lifecycle.input", input_sha256),
            (
                "qqq.options.lifecycle.accounting_file",
                request.cash_accounting_result_file_sha256,
            ),
            ("qqq.options.lifecycle.accounting_snapshot", prior.content_sha256),
            ("qqq.options.lifecycle.calendar", request.exchange_calendar_source_sha256),
        ]
        + [
            (f"qqq.options.lifecycle.event.{index:04d}", item.content_sha256)
            for index, item in enumerate(events)
        ]
    )
    return PortfolioSnapshotRecord.seal(
        schema_name="portfolio_snapshot",
        schema_version="1.0.0",
        run_id=request.run_manifest.run_id,
        record_id=f"lifecycle-portfolio-snapshot-{input_sha256[:24]}",
        created_at_utc=request.evaluation_at_utc,
        producer_version=request.producer_version,
        repository_code_sha=request.run_manifest.repository_code_sha,
        policy_id=loaded.policy.policy_id,
        policy_version=loaded.policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(item[0] for item in sources),
        source_checksums=tuple(item[1] for item in sources),
        requested_start=request.run_manifest.requested_start,
        requested_end=request.run_manifest.requested_end,
        evaluated_start=request.run_manifest.evaluated_start,
        evaluated_end=request.run_manifest.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=request.lineage_id,
        safety=_shared_safety(),
        snapshot_at_utc=request.evaluation_at_utc,
        currency="USD",
        settled_cash_usd=prior.settled_cash_usd,
        unsettled_cash_usd=prior.unsettled_cash_usd,
        reserved_cash_usd=prior.reserved_cash_usd,
        option_market_value_usd=option_market_value,
        fees_paid_usd=prior.fees_paid_usd,
        realized_pnl_usd=prior.realized_pnl_usd - cost_expired,
        unrealized_pnl_usd=prior.unrealized_pnl_usd - unrealized_removed,
    )


def _summaries(all_states: list[_PositionState]) -> tuple[QQQOptionLifecyclePositionSummary, ...]:
    summaries: list[QQQOptionLifecyclePositionSummary] = []
    for state in sorted(all_states, key=lambda item: item.position_id):
        if not state.events:
            raise _ReplayAbort("lifecycle position lacks events")
        cost = (
            Decimal("0")
            if state.state in {"FLAT", "CLOSED"}
            else state.remaining_cost_basis_usd
        )
        summaries.append(
            QQQOptionLifecyclePositionSummary.seal(
                schema_version="qqq_option_lifecycle_position_summary.v1",
                position_id=state.position_id,
                option_sid=state.candidate.option_sid,
                right=state.candidate.right,
                expiry=state.candidate.expiry,
                strike_usd_per_share=state.candidate.strike_usd_per_share,
                contract_multiplier=state.candidate.contract_multiplier,
                terminal_state=state.state,
                contracts_open=state.contracts_open,
                remaining_cost_basis_usd=cost,
                candidate_snapshot_sha256=state.candidate.content_sha256,
                last_event_sha256=state.events[-1].content_sha256,
            )
        )
    return tuple(summaries)


def replay_qqq_option_position_lifecycle(
    request: QQQOptionPositionLifecycleRequest,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionPositionLifecycleResult:
    loaded = load_qqq_options_position_lifecycle_policy(
        policy_path,
        project_root=project_root,
    )
    input_sha256 = build_qqq_option_position_lifecycle_input_sha256(request)
    accounting = request.accounting_result
    if not loaded.policy.lifecycle_authorized:
        return _blocked_result(
            loaded=loaded,
            accounting_result_sha256=accounting.content_sha256,
            input_sha256=input_sha256,
            reason_code="LIFECYCLE_POLICY_REVIEW_REQUIRED",
            authorized=False,
        )
    if (
        not accounting.accounting_authorized
        or accounting.reason_code != "ACCOUNTING_REPLAY_READY"
        or accounting.portfolio_snapshot is None
    ):
        return _blocked_result(
            loaded=loaded,
            accounting_result_sha256=accounting.content_sha256,
            input_sha256=input_sha256,
            reason_code="ACCOUNTING_REPLAY_BLOCKED_CASH_PRESERVED",
            authorized=True,
        )
    try:
        _validate_replay_identity(request, policy=loaded.policy)
        all_states, current = _replay_execution_states(
            request,
            loaded=loaded,
            input_sha256=input_sha256,
        )
        reason_code, expired_positions = _apply_external_and_expiry_safety(
            request,
            loaded=loaded,
            input_sha256=input_sha256,
            all_states=all_states,
            current=current,
        )
        events = tuple(
            event
            for state in sorted(all_states, key=lambda item: item.position_id)
            for event in state.events
        )
        positions = _summaries(all_states)
        invalid = reason_code in {
            "EXPIRY_SCOPE_VIOLATION_INVALID_RUN",
            "EXTERNAL_SCOPE_VIOLATION_INVALID_RUN",
            "UNRESOLVED_EXPIRY_INVALID_RUN",
        }
        snapshot = (
            None
            if invalid
            else _build_downstream_snapshot(
                request,
                loaded=loaded,
                input_sha256=input_sha256,
                expired_positions=expired_positions,
                events=events,
            )
        )
        dq_status = _worst_status(
            [
                request.accounting_result.portfolio_snapshot.dq_status,
                *(item.dq_status for item in events),
            ]
        )
        pit_status = _worst_status(
            [
                request.accounting_result.portfolio_snapshot.pit_status,
                *(item.pit_status for item in events),
            ]
        )
        criteria = loaded.policy.criteria
        assert isinstance(criteria, ActivePositionLifecycleCriteria)
        interpretation_allowed = bool(
            not invalid
            and reason_code in {
                "LIFECYCLE_REPLAY_READY",
                "EXPIRY_CLOSED_WORTHLESS",
            }
            and request.accounting_result.investment_interpretation_allowed
            and criteria.reality_baseline
            and dq_status == "PASS"
            and pit_status == "PASS"
        )
        return QQQOptionPositionLifecycleResult.seal(
            schema_version="qqq_options_position_lifecycle_result.v1",
            policy_sha256=loaded.policy_sha256,
            selection_policy_sha256=loaded.policy.selection_policy_sha256,
            execution_policy_sha256=loaded.policy.execution_policy_sha256,
            accounting_policy_sha256=loaded.policy.accounting_policy_sha256,
            accounting_result_sha256=accounting.content_sha256,
            input_sha256=input_sha256,
            lifecycle_authorized=True,
            investment_interpretation_allowed=interpretation_allowed,
            cash_preservation_required=invalid,
            run_valid=not invalid,
            reason_code=reason_code,
            lifecycle_stage_dq_status=dq_status,
            lifecycle_stage_pit_status=pit_status,
            lifecycle_events=events,
            positions=positions,
            portfolio_snapshot=snapshot,
            new_order_intent_count=0,
            new_fill_count=0,
            safety=loaded.policy.safety,
        )
    except _ReplayAbort:
        return _blocked_result(
            loaded=loaded,
            accounting_result_sha256=accounting.content_sha256,
            input_sha256=input_sha256,
            reason_code="LIFECYCLE_INPUT_INVALID",
            authorized=True,
        )


__all__ = [
    "ActivePositionLifecycleCriteria",
    "DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH",
    "LifecycleExternalEventType",
    "LifecycleObservationKind",
    "LifecycleReason",
    "LifecycleScenarioRole",
    "QQQOptionCandidateSnapshotArtifact",
    "QQQOptionExecutionResultArtifact",
    "QQQOptionLifecycleExternalEvent",
    "QQQOptionLifecycleMarketObservation",
    "QQQOptionLifecyclePositionSummary",
    "QQQOptionPositionLifecycleContractError",
    "QQQOptionPositionLifecyclePolicy",
    "QQQOptionPositionLifecyclePolicyLoadResult",
    "QQQOptionPositionLifecycleRequest",
    "QQQOptionPositionLifecycleResult",
    "QQQOptionPositionLifecycleSafety",
    "UnresolvedPositionLifecycleCriteria",
    "build_qqq_option_position_lifecycle_input_sha256",
    "load_qqq_options_position_lifecycle_policy",
    "replay_qqq_option_position_lifecycle",
]
